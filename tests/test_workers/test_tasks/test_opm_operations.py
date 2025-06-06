# SPDX-License-Identifier: GPL-3.0-or-later
import os.path
import pytest
import textwrap
import socket

from unittest import mock

from iib.exceptions import IIBError, AddressAlreadyInUse
from iib.workers.config import get_worker_config
from iib.workers.tasks import opm_operations
from iib.workers.tasks.iib_static_types import BundleImage
from iib.workers.tasks.opm_operations import (
    Opm,
    PortFileLock,
    create_port_filelocks,
    get_opm_port_stacks,
    PortFileLockGenerator,
    _get_input_data_path,
    _get_olm_bundle_version,
    get_list_bundles,
    get_operator_package_list,
)


@pytest.fixture()
def mock_config():
    with mock.patch('iib.workers.tasks.opm_operations.get_worker_config') as mc:
        mock_config = mock.MagicMock()
        mock_config.iib_opm_port_ranges = {
            'opm_port': (5001, 5003),
            'opm_pprof_port': (6001, 6003),
        }
        mock_config.iib_opm_pprof_lock_required_min_version = "0.9.0"
        mock_config.iib_grpc_init_wait_time = 1
        mock_config.iib_grpc_max_port_tries = 3
        mock_config.iib_grpc_max_tries = 3
        mock_config.iib_deprecate_bundles_limit = 5
        mc.return_value = mock_config
        yield mc


@pytest.fixture(autouse=True)
def ensure_opm_default():
    # Fixture to ensure every test starts with a default opm
    opm_operations.Opm.opm_version = get_worker_config().get('iib_default_opm')


@mock.patch('tempfile.gettempdir', return_value='/tmp')
def test_PortFileLock_initialization(mock_tempdir):
    """Test PortFileLock __init__ method."""
    pfl = PortFileLock("test_purpose", 5000)
    assert pfl.purpose == "test_purpose"
    assert pfl.port == 5000
    assert not pfl.locked
    assert pfl.filename == '/tmp/iib_test_purpose_5000.lock'


@mock.patch('tempfile.gettempdir', return_value='/tmp')
def test_PortFileLock_repr(mock_tempdir):
    """Check PortFileLock __repr__() returned string."""
    pfl = PortFileLock("test_purpose", 5000)
    assert str(pfl) == "PortFileLock(port: 5000, purpose: test_purpose, locked: False)"


@mock.patch('os.close')
@mock.patch('os.open', return_value="42")
@mock.patch('socket.socket')
@mock.patch('tempfile.gettempdir', return_value='/tmp')
def test_lock_acquire_success(mock_tempdir, mock_socket, mock_open, mock_close):
    """Test succesfull lock acquisition."""
    pfl = PortFileLock("test_purpose", 5000)
    pfl.lock_acquire()

    mock_socket.return_value.bind.assert_called_once_with(("localhost", 5000))
    mock_open.assert_called_once_with(
        '/tmp/iib_test_purpose_5000.lock',
        os.O_CREAT | os.O_EXCL,
    )
    # 42 is mocked FD from mock_open
    mock_close.assert_called_once_with('42')
    assert pfl.locked


@mock.patch('os.open', side_effect=FileExistsError)
@mock.patch('socket.socket')
@mock.patch('tempfile.gettempdir', return_value='/tmp')
def test_lock_acquire_port_already_iib_locked(mock_tempdir, mock_socket, mock_open):
    """Test unsuccesfull lock, due to other IIB worker using this port."""
    pfl = PortFileLock("test_purpose", 5000)

    with pytest.raises(
        AddressAlreadyInUse,
        match="Port 5000 is already locked by other IIB worker.",
    ):
        pfl.lock_acquire()

    mock_socket.return_value.bind.assert_called_once_with(("localhost", 5000))
    mock_open.assert_called_once_with(
        '/tmp/iib_test_purpose_5000.lock',
        os.O_CREAT | os.O_EXCL,
    )
    assert pfl.locked is False


@mock.patch('os.close')
@mock.patch('os.open')
@mock.patch('socket.socket')
@mock.patch('tempfile.gettempdir', return_value='/tmp')
def test_lock_acquire_port_in_use(mock_tempdir, mock_socket, mock_open, mock_close):
    """Test unsuccesfull lock, due to other service using this port."""
    mock_socket.return_value.bind.side_effect = socket.error
    pfl = PortFileLock("test_purpose", 5000)

    with pytest.raises(AddressAlreadyInUse, match="Port 5000 is already in use"):
        pfl.lock_acquire()

    assert not pfl.locked


@mock.patch('os.remove')
@mock.patch('os.close')
@mock.patch('os.open')
@mock.patch('socket.socket')
@mock.patch('tempfile.gettempdir', return_value='/tmp')
def test_unlock(mock_tempdir, mock_socket, mock_open, mock_close, mock_remove):
    """Test PortFileLock unlock method."""
    pfl = PortFileLock("test_purpose", 5000)

    # Attempt to unlock, not locked PortFileLock
    err_msg = (
        r'Attempt to unlock not-locked PortFileLock'
        r'\(port: 5000, purpose: test_purpose, locked: False\).'
    )
    with pytest.raises(IIBError, match=err_msg):
        pfl.unlock()
    pfl.lock_acquire()
    pfl.unlock()
    assert not pfl.locked
    mock_remove.assert_called_once_with('/tmp/iib_test_purpose_5000.lock')


@mock.patch('iib.workers.tasks.opm_operations.PortFileLock', autospec=True)
def test_PortFileLockGenerator_success(mock_pfl):
    """Test port_file_locks_generator()."""
    port_stacks = [[5000, 6000], [5001, 6001]]
    port_purposes = ['purpose1', 'purpose2']

    port_file_locks_generator = PortFileLockGenerator(
        port_stacks=port_stacks,
        port_purposes=port_purposes,
    )

    # First generation
    locks = port_file_locks_generator.get_new_locks()
    assert len(locks) == 2
    mock_pfl.assert_any_call(purpose='purpose1', port=5000)
    mock_pfl.assert_any_call(purpose='purpose2', port=6000)

    # Second generation
    locks = port_file_locks_generator.get_new_locks()
    assert len(locks) == 2
    mock_pfl.assert_any_call(purpose='purpose1', port=5001)
    mock_pfl.assert_any_call(purpose='purpose2', port=6001)


def test_PortFileLockGenerator_no_ports_available():
    """Test port_file_locks_generator() exception."""
    port_stacks = []
    port_purposes = ['purpose1', 'purpose2']

    port_file_locks_generator = PortFileLockGenerator(
        port_stacks=port_stacks,
        port_purposes=port_purposes,
    )

    err_msg = 'No free port has been found after 0 attempts.'
    with pytest.raises(IIBError, match=err_msg):
        port_file_locks_generator.get_new_locks()


@pytest.mark.parametrize(
    'expected_ports, expected_purposes, opm_version',
    [
        ([[5001, 6001], [5002, 6002]], ['opm_port', 'opm_pprof_port'], '0.9.0'),
        ([[5001], [5002]], ['opm_port'], '0.8.0'),
    ],
)
@mock.patch('iib.workers.tasks.opm_operations.Opm.get_opm_version_number')
def test_get_opm_port_stacks(
    mock_opm_gov,
    opm_version,
    expected_purposes,
    expected_ports,
    mock_config,
):
    """Test get_opm_port_stacks() is working correctly considering OPM opm_version attribute."""
    mock_opm_gov.return_value = opm_version

    ports, purposes = get_opm_port_stacks(['opm_port', 'opm_pprof_port'])
    assert sorted(ports) == expected_ports
    assert purposes == expected_purposes


@mock.patch(
    'iib.workers.tasks.opm_operations.get_opm_port_stacks',
    return_value=(
        [[5001, 6001], [5002, 6002]],
        ['opm_port', 'opm_pprof_port'],
    ),
)
@mock.patch('iib.workers.tasks.opm_operations.PortFileLock.lock_acquire')
@mock.patch('iib.workers.tasks.opm_operations.PortFileLock.unlock')
def test_create_port_filelocks_success(mock_pfl_u, mock_pfl_la, mock_gops):
    """Test the create_port_filelocks decorator when port locks are successfully acquired."""

    @create_port_filelocks(port_purposes=['opm_port', 'opm_pprof_port'])
    def test_func(argument, opm_port, opm_pprof_port=None):
        assert argument == 'test'
        assert opm_port == 5001
        assert opm_pprof_port == 6001

    test_func(argument="test")

    assert mock_pfl_la.call_count == 2
    assert mock_pfl_u.call_count == 2


@mock.patch(
    'iib.workers.tasks.opm_operations.get_opm_port_stacks',
    return_value=(
        [[5001, 6001], [5002, 6002], [5003, 6003]],
        ['opm_port', 'opm_pprof_port'],
    ),
)
@mock.patch(
    'iib.workers.tasks.opm_operations.PortFileLock.lock_acquire',
    side_effect=[None, AddressAlreadyInUse, AddressAlreadyInUse, None, None, None],
)
@mock.patch('iib.workers.tasks.opm_operations.PortFileLock.unlock')
def test_create_port_filelocks_retry(mock_pfl_u, mock_pfl_la, mock_gmock_gops):
    """Test the create_port_filelocks decorator retries when a port is already in use."""

    @create_port_filelocks(port_purposes=['opm_port', 'opm_pprof_port'])
    def test_func(argument, opm_port, opm_pprof_port=None):
        assert argument == 'test'
        assert opm_port == 5003
        assert opm_pprof_port == 6003

    test_func(argument="test")

    assert mock_pfl_la.call_count == 5
    assert mock_pfl_u.call_count == 3


@mock.patch(
    'iib.workers.tasks.opm_operations.get_opm_port_stacks',
    return_value=(
        [[5001, 6001]],
        ['opm_port', 'opm_pprof_port'],
    ),
)
@mock.patch(
    'iib.workers.tasks.opm_operations.PortFileLock.lock_acquire',
    side_effect=[AddressAlreadyInUse],
)
@mock.patch('iib.workers.tasks.opm_operations.PortFileLock.unlock')
def test_create_port_filelocks_failure(mock_pfl_u, mock_pfl_la, mock_gops):
    """Test the create_port_filelocks decorator when whole port stack is already in use."""
    test_argument = "test"

    @create_port_filelocks(port_purposes=['opm_port', 'opm_pprof_port'])
    def test_func(argument, opm_port, opm_pprof_port=None):
        assert argument == test_argument
        assert opm_port == 5001
        assert opm_pprof_port == 6001

    with pytest.raises(IIBError, match="No free port has been found after 1 attempts."):
        test_func(argument=test_argument)

    assert mock_pfl_la.call_count == 1
    assert mock_pfl_u.call_count == 0


@pytest.mark.parametrize(
    "opm_version,migrate_args",
    [
        ("v1.26.8", []),
        ("v1.47.2", ['--migrate-level', 'bundle-object-to-csv-metadata']),
    ],
)
@mock.patch('iib.workers.tasks.opm_operations.opm_validate')
@mock.patch('iib.workers.tasks.opm_operations.shutil.rmtree')
@mock.patch('iib.workers.tasks.opm_operations.generate_cache_locally')
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_opm_migrate(
    mock_run_cmd,
    mock_gcl,
    moch_srmtree,
    mock_opmvalidate,
    opm_version,
    migrate_args,
    monkeypatch,
    tmpdir,
):
    monkeypatch.setattr(opm_operations.Opm, 'opm_version', f'opm-{opm_version}')
    monkeypatch.setattr(opm_operations.Opm, 'get_opm_version_number', lambda: opm_version)

    index_db_file = os.path.join(tmpdir, 'database/index.db')

    opm_operations.opm_migrate(index_db_file, tmpdir)
    moch_srmtree.assert_not_called()

    fbc_dir = os.path.join(tmpdir, 'catalog')

    mock_run_cmd.assert_called_once_with(
        [f'opm-{opm_version}', 'migrate', *migrate_args, index_db_file, fbc_dir],
        {'cwd': tmpdir},
        exc_msg='Failed to migrate index.db to file-based catalog',
    )

    mock_opmvalidate.assert_called_once_with(fbc_dir)
    mock_gcl.assert_called_once_with(tmpdir, fbc_dir, mock.ANY)


@pytest.mark.parametrize("dockerfile", (None, 'index.Dockerfile'))
def test_create_dockerfile_binary(tmpdir, dockerfile):
    index_db_file = os.path.join(tmpdir, 'database/index.db')
    fbc_dir = os.path.join(tmpdir, 'catalogs')

    opm_operations.create_dockerfile(
        fbc_dir, tmpdir, index_db_file, "some:image", dockerfile_name=dockerfile
    )

    df_name = dockerfile if dockerfile else f"{os.path.basename(fbc_dir)}.Dockerfile"
    df_path = os.path.join(tmpdir, df_name)
    with open(df_path, 'r') as f:
        dockerfile = f.read()

    expected_dockerfile = textwrap.dedent(
        '''\
        FROM some:image

        # Configure the entrypoint and command
        ENTRYPOINT ["/bin/opm"]
        CMD ["serve", "/configs", "--cache-dir=/tmp/cache"]

        # Copy declarative config root and cache into image
        ADD catalogs /configs
        COPY --chown=1001:0 cache /tmp/cache

        # Set DC-specific label for the location of the DC root directory
        # in the image
        LABEL operators.operatorframework.io.index.configs.v1=/configs

        ADD database/index.db /var/lib/iib/_hidden/do.not.edit.db
        '''
    )
    assert dockerfile == expected_dockerfile


@pytest.mark.parametrize("dockerfile", (None, 'index.Dockerfile'))
def test_create_dockerfile_binaryless(tmpdir, dockerfile):
    index_db_file = os.path.join(tmpdir, 'database/index.db')
    fbc_dir = os.path.join(tmpdir, 'catalogs')

    opm_operations.create_dockerfile(
        fbc_dir, tmpdir, index_db_file, "scratch", dockerfile_name=dockerfile
    )

    df_name = dockerfile if dockerfile else f"{os.path.basename(fbc_dir)}.Dockerfile"
    df_path = os.path.join(tmpdir, df_name)
    with open(df_path, 'r') as f:
        dockerfile = f.read()

    expected_dockerfile = textwrap.dedent(
        '''\
        FROM scratch

        # Copy declarative config root and cache into image
        ADD catalogs /configs
        COPY --chown=1001:0 cache /tmp/cache

        # Set DC-specific label for the location of the DC root directory
        # in the image
        LABEL operators.operatorframework.io.index.configs.v1=/configs

        ADD database/index.db /var/lib/iib/_hidden/do.not.edit.db
        '''
    )
    assert dockerfile == expected_dockerfile


@pytest.mark.parametrize("set_index_db_file", (False, True))
@pytest.mark.parametrize("dockerfile", (None, 'index.Dockerfile'))
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_create_dockerfile_exist(mock_run_cmd, tmpdir, dockerfile, set_index_db_file):
    index_db_file = os.path.join(tmpdir, 'database/index.db') if set_index_db_file else None
    fbc_dir = os.path.join(tmpdir, 'catalogs')
    df_name = f"{os.path.basename(fbc_dir)}.Dockerfile" if not dockerfile else dockerfile
    df_path = os.path.join(tmpdir, df_name)

    # create Dockerfile for tests
    with open(df_path, 'a'):
        pass

    opm_operations.create_dockerfile(
        fbc_dir, tmpdir, index_db_file, "some:image", dockerfile_name=dockerfile
    )

    mock_run_cmd.assert_not_called()


@pytest.mark.parametrize('from_index', (None, 'some_index:latest'))
@pytest.mark.parametrize('bundles', (['bundle:1.2', 'bundle:1.3'], []))
@pytest.mark.parametrize('overwrite_csv', (True, False))
@pytest.mark.parametrize('container_tool', (None, 'podwoman'))
@mock.patch('iib.workers.tasks.utils.set_registry_token')
@mock.patch('iib.workers.tasks.utils.run_cmd')
@mock.patch('iib.workers.tasks.opm_operations.shutil.copyfile')
@mock.patch('iib.workers.tasks.opm_operations.os.remove')
def test_opm_registry_add(
    mock_os_remove,
    mock_shutil_copyfile,
    mock_run_cmd,
    mock_srt,
    from_index,
    bundles,
    overwrite_csv,
    container_tool,
):
    opm_operations._opm_registry_add(
        base_dir='/tmp/somedir',
        index_db='/tmp/somedir/some.db',
        bundles=bundles,
        overwrite_csv=overwrite_csv,
        container_tool=container_tool,
    )

    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[:3] == ['opm', 'registry', 'add']
    if bundles:
        assert ','.join(bundles) in opm_args
    else:
        assert '""' in opm_args
    if overwrite_csv:
        assert '--overwrite-latest' in opm_args
    else:
        assert '--overwrite-latest' not in opm_args
    if container_tool:
        assert '--container-tool' in opm_args
        assert container_tool in opm_args
    else:
        assert '--container-tool' not in opm_args
    assert "--enable-alpha" in opm_args


@pytest.mark.parametrize('is_fbc', (True, False))
@pytest.mark.parametrize('from_index', (None, 'some_index:latest'))
@pytest.mark.parametrize('bundles', (['bundle:1.2', 'bundle:1.3'], []))
@pytest.mark.parametrize('overwrite_csv', (True, False))
@pytest.mark.parametrize('container_tool', (None, 'podwoman'))
@pytest.mark.parametrize('graph_update_mode', (None, 'semver-skippatch'))
@mock.patch('iib.workers.tasks.opm_operations.create_dockerfile')
@mock.patch('iib.workers.tasks.opm_operations.opm_migrate')
@mock.patch('iib.workers.tasks.opm_operations._opm_registry_add')
@mock.patch('iib.workers.tasks.build._get_index_database')
@mock.patch('iib.workers.tasks.opm_operations.get_hidden_index_database')
@mock.patch('iib.workers.tasks.opm_operations.is_image_fbc')
def test_opm_registry_add_fbc(
    mock_iifbc,
    mock_ghid,
    mock_gid,
    mock_ora,
    mock_om,
    mock_ogd,
    from_index,
    bundles,
    overwrite_csv,
    container_tool,
    graph_update_mode,
    is_fbc,
    tmpdir,
):
    index_db_file = os.path.join(tmpdir, 'database/index.db')
    fbc_dir = os.path.join(tmpdir, 'catalogs')
    cache_dir = os.path.join(tmpdir, 'cache')
    mock_ghid.return_value = index_db_file
    mock_gid.return_value = index_db_file
    mock_om.return_value = (fbc_dir, cache_dir)
    mock_iifbc.return_value = is_fbc

    opm_operations.opm_registry_add_fbc(
        base_dir=tmpdir,
        bundles=bundles,
        binary_image="some:image",
        from_index=from_index,
        graph_update_mode=graph_update_mode,
        overwrite_csv=overwrite_csv,
        container_tool=container_tool,
    )

    mock_ora.assert_called_once_with(
        base_dir=tmpdir,
        index_db=index_db_file,
        bundles=bundles,
        overwrite_csv=overwrite_csv,
        container_tool=container_tool,
        graph_update_mode=graph_update_mode,
    )

    mock_om.assert_called_once_with(index_db=index_db_file, base_dir=tmpdir)
    mock_ogd.assert_called_once_with(
        fbc_dir=fbc_dir,
        base_dir=tmpdir,
        index_db=index_db_file,
        binary_image="some:image",
        dockerfile_name='index.Dockerfile',
    )


@pytest.mark.parametrize('operators', (['abc-operator', 'xyz-operator'], []))
@mock.patch('iib.workers.tasks.opm_operations.create_dockerfile')
@mock.patch('iib.workers.tasks.opm_operations.opm_migrate')
@mock.patch('iib.workers.tasks.opm_operations._opm_registry_rm')
@mock.patch('iib.workers.tasks.opm_operations.get_hidden_index_database')
@mock.patch('iib.workers.tasks.utils.set_registry_token')
def test_opm_registry_rm_fbc(
    mock_srt,
    mock_ghid,
    mock_orr,
    mock_om,
    mock_ogd,
    tmpdir,
    operators,
):
    from_index = 'some_index:latest'
    index_db_file = os.path.join(tmpdir, 'database/index.db')
    fbc_dir = os.path.join(tmpdir, 'catalogs')
    mock_ghid.return_value = index_db_file
    mock_om.return_value = (fbc_dir, None)

    opm_operations.opm_registry_rm_fbc(tmpdir, from_index, operators, index_db_file)

    mock_orr.assert_called_once_with(
        index_db_path=index_db_file, operators=operators, base_dir=tmpdir
    )

    mock_om.assert_called_once_with(index_db=index_db_file, base_dir=tmpdir, generate_cache=False)


@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_opm_registry_rm(mock_run_cmd):
    packages = ['abc-operator', 'xyz-operator']
    opm_operations._opm_registry_rm(
        '/tmp/somedir/some.db',
        packages,
        '/tmp/somedir',
    )

    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[:3] == ['opm', 'registry', 'rm']
    assert ','.join(packages) in opm_args


@pytest.mark.parametrize(
    'from_index, is_fbc', [('some-fbc-index:latest', True), ('some-sqlite-index:latest', False)]
)
@mock.patch('iib.workers.tasks.opm_operations.create_dockerfile')
@mock.patch('iib.workers.tasks.opm_operations.opm_migrate')
@mock.patch('iib.workers.tasks.opm_operations._opm_registry_rm')
@mock.patch('iib.workers.tasks.opm_operations.get_hidden_index_database')
@mock.patch('iib.workers.tasks.build._get_index_database')
@mock.patch('iib.workers.tasks.opm_operations.is_image_fbc')
@mock.patch('iib.workers.tasks.opm_operations.set_request_state')
def test_opm_create_empty_fbc(
    mock_srs, mock_iif, mock_gid, mock_ghid, mock_orr, mock_om, mock_ogd, tmpdir, from_index, is_fbc
):

    operators = ['abc-operator', 'xyz-operator']
    mock_iif.return_value = is_fbc
    hidden_index_db_file = os.path.join(tmpdir, 'hidden/index.db')
    fbc_dir = os.path.join(tmpdir, 'catalogs')
    cache_dir = os.path.join(tmpdir, 'cache')
    mock_ghid.return_value = hidden_index_db_file
    mock_om.return_value = (fbc_dir, cache_dir)

    index_db_file = os.path.join(tmpdir, 'database/index.db')
    mock_gid.return_value = index_db_file

    opm_operations.opm_create_empty_fbc(3, tmpdir, from_index, from_index, 'some:image', operators)

    if is_fbc:
        mock_orr.assert_called_once_with(
            index_db_path=hidden_index_db_file, operators=operators, base_dir=tmpdir
        )
        mock_ghid.assert_called_once()
    else:
        mock_gid.assert_called_once()
        mock_orr.assert_called_once_with(
            index_db_path=index_db_file, operators=operators, base_dir=tmpdir
        )


@pytest.mark.parametrize("from_index", (None, "image:latest"))
@pytest.mark.parametrize("db_exist", (True, False))
@mock.patch('iib.workers.tasks.opm_operations.is_image_fbc')
@mock.patch('iib.workers.tasks.opm_operations.get_hidden_index_database')
def test_get_or_create_temp_index_db_file(mock_ghid, mock_iifbc, db_exist, from_index, tmpdir):
    def create_index_db(*args, **kwargs):
        db_file = os.path.join(tmpdir, get_worker_config()['temp_index_db_path'])
        os.makedirs(os.path.dirname(db_file), exist_ok=True)
        with open(db_file, 'w'):
            pass
        return db_file

    mock_ghid.side_effect = create_index_db
    mock_iifbc.return_value = True

    if db_exist:
        create_index_db()

    index_db_file = opm_operations._get_or_create_temp_index_db_file(
        base_dir=tmpdir, from_index=from_index
    )
    assert os.path.isfile(index_db_file)


@pytest.mark.parametrize('bundles', (['bundle:1.2', 'bundle:1.3'], []))
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_opm_registry_deprecatetruncate(mock_run_cmd, bundles):
    index_db_file = '/tmp/test_file.db'
    cmd = [
        'opm',
        'registry',
        'deprecatetruncate',
        '--database',
        index_db_file,
        '--bundle-images',
        ','.join(bundles),
        '--allow-package-removal',
    ]

    opm_operations.opm_registry_deprecatetruncate(
        base_dir='/tmp',
        index_db=index_db_file,
        bundles=bundles,
    )

    mock_run_cmd.assert_called_once_with(
        cmd, {'cwd': '/tmp'}, exc_msg=f'Failed to deprecate the bundles on {index_db_file}'
    )


@pytest.mark.parametrize('bundles', (['bundle:1.2', 'bundle:1.3'], []))
@pytest.mark.parametrize('from_index', (None, 'some_index:latest'))
@mock.patch('iib.workers.tasks.opm_operations.create_dockerfile')
@mock.patch('iib.workers.tasks.opm_operations.opm_migrate')
@mock.patch('iib.workers.tasks.opm_operations.opm_registry_deprecatetruncate')
@mock.patch('iib.workers.tasks.opm_operations._get_or_create_temp_index_db_file')
def test_deprecate_bundles_fbc(
    mock_gtidf,
    mock_ord,
    mock_om,
    mock_cd,
    from_index,
    bundles,
    tmpdir,
):
    index_db_file = os.path.join(tmpdir, 'database/index.db')
    fbc_dir = os.path.join(tmpdir, 'catalogs')
    cache_dir = os.path.join(tmpdir, 'cache')
    mock_gtidf.return_value = index_db_file
    mock_om.return_value = (fbc_dir, cache_dir)

    opm_operations.deprecate_bundles_fbc(
        bundles=bundles,
        base_dir=tmpdir,
        binary_image="some:image",
        from_index=from_index,
    )
    if bundles:
        mock_ord.assert_called_once_with(base_dir=tmpdir, index_db=index_db_file, bundles=bundles)

    mock_om.assert_called_once_with(index_db_file, tmpdir)
    mock_cd.assert_called_once_with(
        fbc_dir=fbc_dir,
        base_dir=tmpdir,
        index_db=index_db_file,
        binary_image="some:image",
        dockerfile_name='index.Dockerfile',
    )


@pytest.mark.parametrize(
    'bundles, expected_call_count',
    [
        ([], 0),
        (['bundle:' + str(i + 1) + ".1" for i in range(1)], 1),
        (['bundle:' + str(i + 1) + ".1" for i in range(5)], 1),
        (['bundle:' + str(i + 1) + ".1" for i in range(6)], 2),
    ],
)
@mock.patch('iib.workers.tasks.opm_operations.create_dockerfile')
@mock.patch('iib.workers.tasks.opm_operations.opm_migrate')
@mock.patch('iib.workers.tasks.opm_operations.opm_registry_deprecatetruncate')
@mock.patch('iib.workers.tasks.opm_operations._get_or_create_temp_index_db_file')
def test_deprecate_bundles_fbc_too_many_bundles(
    mock_gtidf,
    mock_ord,
    mock_om,
    mock_cd,
    bundles,
    expected_call_count,
    tmpdir,
    mock_config,
):
    from_index = 'some_index:latest'
    index_db_file = os.path.join(tmpdir, 'database/index.db')
    iib_deprecate_bundles_limit = 5  # Setup as a mock for opm_operations get_config
    fbc_dir = os.path.join(tmpdir, 'catalogs')
    cache_dir = os.path.join(tmpdir, 'cache')
    mock_gtidf.return_value = index_db_file
    mock_om.return_value = (fbc_dir, cache_dir)

    opm_operations.deprecate_bundles_fbc(
        base_dir=tmpdir,
        binary_image="some:image",
        bundles=bundles,
        from_index=from_index,
    )

    expected_calls = [
        mock.call(
            base_dir=tmpdir,
            bundles=bundles[i : i + iib_deprecate_bundles_limit],
            index_db=index_db_file,
        )
        for i in range(0, len(bundles), iib_deprecate_bundles_limit)
    ]

    mock_ord.assert_has_calls(expected_calls)
    assert mock_ord.call_count == expected_call_count


@mock.patch('iib.workers.tasks.utils.run_cmd')
@mock.patch('iib.workers.tasks.opm_operations.os.path.isdir', return_value=True)
@mock.patch(
    'iib.workers.tasks.opm_operations.get_opm_port_stacks',
    return_value=(
        [[6001], [6002]],
        ['opm_pprof_port'],
    ),
)
@mock.patch('iib.workers.tasks.opm_operations.PortFileLock.lock_acquire')
@mock.patch('iib.workers.tasks.opm_operations.PortFileLock.unlock')
def test_generate_cache_locally(
    mock_pfl_u,
    mock_pfl_la,
    mock_gops,
    mock_isdir,
    mock_cmd,
    tmpdir,
):
    fbc_dir = os.path.join(tmpdir, 'catalogs')
    local_cache_path = os.path.join(tmpdir, 'cache')
    cmd = [
        'opm',
        'serve',
        os.path.abspath(fbc_dir),
        f'--cache-dir={local_cache_path}',
        '--cache-only',
        '--termination-log',
        '/dev/null',
    ]

    opm_operations.generate_cache_locally(tmpdir, fbc_dir, local_cache_path)

    cmd.extend(['--pprof-addr', '127.0.0.1:6001'])

    mock_cmd.assert_called_once_with(
        cmd,
        {'cwd': tmpdir},
        exc_msg='Failed to generate cache for file-based catalog',
    )


@mock.patch('iib.workers.tasks.utils.run_cmd')
@mock.patch(
    'iib.workers.tasks.opm_operations.get_opm_port_stacks',
    return_value=([None], []),
)
@mock.patch('iib.workers.tasks.opm_operations.PortFileLock.lock_acquire')
@mock.patch('iib.workers.tasks.opm_operations.PortFileLock.unlock')
def test_generate_cache_locally_failed(
    mock_pfl_u,
    mock_pfl_la,
    mock_gops,
    mock_cmd,
    tmpdir,
):
    fbc_dir = os.path.join(tmpdir, 'catalogs')
    local_cache_path = os.path.join(tmpdir, 'cache')
    cmd = [
        'opm',
        'serve',
        os.path.abspath(fbc_dir),
        f'--cache-dir={local_cache_path}',
        '--cache-only',
        '--termination-log',
        '/dev/null',
    ]

    with pytest.raises(IIBError, match='Cannot find generated cache at .+'):
        opm_operations.generate_cache_locally(tmpdir, fbc_dir, local_cache_path)
        mock_cmd.assert_called_once_with(
            cmd, {'cwd': tmpdir}, exc_msg='Failed to generate cache for file-based catalog'
        )


@pytest.mark.parametrize(
    'operators_exists, index_db_path',
    [(['test-operator'], "index_path"), ([], "index_path")],
)
@mock.patch('iib.workers.tasks.opm_operations.create_dockerfile')
@mock.patch('iib.workers.tasks.opm_operations.generate_cache_locally')
@mock.patch('iib.workers.tasks.opm_operations.shutil.copytree')
@mock.patch('iib.workers.tasks.opm_operations.os.listdir')
@mock.patch('iib.workers.tasks.opm_operations.opm_migrate')
@mock.patch('iib.workers.tasks.opm_operations._opm_registry_rm')
@mock.patch('iib.workers.tasks.opm_operations.get_catalog_dir')
@mock.patch('iib.workers.tasks.opm_operations.verify_operators_exists')
@mock.patch('iib.workers.tasks.opm_operations.extract_fbc_fragment')
@mock.patch('iib.workers.tasks.opm_operations.set_request_state')
def test_opm_registry_add_fbc_fragment(
    mock_srs,
    mock_eff,
    mock_voe,
    mock_gcr,
    mock_orr,
    mock_om,
    mock_ldr,
    mock_cpt,
    mock_gcc,
    mock_ogd,
    operators_exists,
    index_db_path,
    tmpdir,
):
    configs_dir = tmpdir.mkdir('configs')
    from_index = "example.com/test/index"
    binary_image = "example.com/ose/binary"
    fbc_fragment = "example.com/test/fragment"
    fbc_fragment_operators = ["test-operator"]
    mock_eff.return_value = (os.path.join(tmpdir, "fbc_fragment"), fbc_fragment_operators)
    mock_voe.return_value = operators_exists, index_db_path
    mock_gcr.return_value = configs_dir
    mock_om.return_value = os.path.join(tmpdir, "catalog"), None
    mock_ldr.return_value = [
        "package1",
    ]

    deprecations_dir = configs_dir.mkdir(get_worker_config()['operator_deprecations_dir'])
    operator_deprecation_dir = deprecations_dir.mkdir('test-operator')
    deprecation_template = textwrap.dedent(
        """\
        {{
        "schema": "olm.deprecations",
        "package": "{package_name}",
        "entries": [
            {{
            "reference": {{
                "name": "my-operator.v1.57.7",
                "schema": "olm.bundle"
            }},
            "message": "my-operator.v1.57.7 is deprecated.\n"
            }}
        ]
        }}
        """
    )
    if operators_exists:
        deprecation_file = operator_deprecation_dir.join('test-operator.json')
        deprecation_file.write(deprecation_template.format(package_name='test-operator'))
    else:
        # Make sure deprecations for other operators are not removed
        deprecation_file = operator_deprecation_dir.join('other-operator.json')
        deprecation_file.write(deprecation_template.format(package_name='other-operator'))

    # Assert deprecations file was created as expected
    assert os.path.exists(deprecation_file)

    opm_operations.opm_registry_add_fbc_fragment(
        10, tmpdir, from_index, binary_image, fbc_fragment, None
    )

    mock_eff.assert_called_with(temp_dir=tmpdir, fbc_fragment=fbc_fragment)
    mock_voe.assert_called_with(
        from_index=from_index,
        base_dir=tmpdir,
        operator_packages=fbc_fragment_operators,
        overwrite_from_index_token=None,
    )
    mock_gcr.assert_called_with(from_index=from_index, base_dir=tmpdir)
    if operators_exists:
        mock_orr.assert_called_with(
            index_db_path=index_db_path, operators=fbc_fragment_operators, base_dir=tmpdir
        )
        mock_om.assert_called_with(index_db=index_db_path, base_dir=tmpdir, generate_cache=False)
        mock_cpt.assert_has_calls(
            [
                mock.call(
                    os.path.join(tmpdir, "catalog", mock_ldr.return_value[0]),
                    os.path.join(tmpdir, "configs", mock_ldr.return_value[0]),
                    dirs_exist_ok=True,
                ),
            ]
        )
        assert mock_cpt.call_count == 2
        # Assert deprecations were removed correctly
        assert not os.path.exists(deprecation_file)
        assert not operator_deprecation_dir.check()
        assert deprecations_dir.check(dir=True)
    else:
        assert mock_cpt.call_count == 1
        assert mock_orr.call_count == 0
        # Assert deprecations are still present
        assert os.path.exists(deprecation_file)
        assert operator_deprecation_dir.check()
        assert deprecations_dir.check(dir=True)
    mock_srs.call_count == 2
    mock_cpt.assert_has_calls(
        [
            mock.call(
                os.path.join(tmpdir, "fbc_fragment", fbc_fragment_operators[0]),
                os.path.join(tmpdir, "configs", fbc_fragment_operators[0]),
            )
        ]
    )
    mock_gcc.assert_called_once()
    mock_ogd.assert_called_once()


@pytest.mark.parametrize(
    'bundles_in_db, opr_exists',
    [
        (
            [
                {"packageName": "test-operator", "version": "v1.0", "bundlePath": "bundle1"},
                {"packageName": "test-operator", "version": "v1.2", "bundlePath": "bundle1"},
                {"packageName": "package2", "version": "v2.0", "bundlePath": "bundle2"},
            ],
            {"test-operator"},
        ),
        (
            [
                {"packageName": "test-operator", "version": "v1.0", "bundlePath": "bundle1"},
                {"packageName": "package2", "version": "v2.0", "bundlePath": "bundle2"},
            ],
            {"test-operator"},
        ),
        (
            [
                {"packageName": "package1", "version": "v1.0", "bundlePath": "bundle1"},
                {"packageName": "package2", "version": "v2.0", "bundlePath": "bundle2"},
            ],
            set(),
        ),
    ],
)
@mock.patch('iib.workers.tasks.utils.run_cmd')
@mock.patch('iib.workers.tasks.opm_operations.get_list_bundles')
@mock.patch('iib.workers.tasks.build._copy_files_from_image')
@mock.patch('iib.workers.tasks.utils.set_registry_token')
def test_verify_operator_exists(
    mock_srt, mock_cffi, mock_glb, mock_rc, bundles_in_db, opr_exists, tmpdir
):
    from_index = "example.com/test/index"
    mock_glb.return_value = bundles_in_db
    package_exists, index_db_path = opm_operations.verify_operators_exists(
        from_index, tmpdir, ['test-operator'], None
    )
    mock_glb.assert_has_calls([mock.call(input_data=index_db_path, base_dir=tmpdir)])
    assert package_exists == opr_exists


@pytest.mark.parametrize('from_index', (None, 'some_index:latest'))
@pytest.mark.parametrize('bundles', (['bundle:1.2', 'bundle:1.3'], []))
@pytest.mark.parametrize('overwrite_csv', (True, False))
@pytest.mark.parametrize('container_tool', (None, 'podwoman'))
@pytest.mark.parametrize('graph_update_mode', (None, 'semver'))
@mock.patch('iib.workers.tasks.utils.set_registry_token')
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_opm_index_add(
    mock_run_cmd,
    mock_srt,
    from_index,
    bundles,
    overwrite_csv,
    container_tool,
    graph_update_mode,
    tmpdir,
):
    opm_operations.opm_index_add(
        '/tmp/somedir',
        bundles,
        'binary-image:latest',
        from_index,
        graph_update_mode,
        'user:pass',
        overwrite_csv,
        container_tool=container_tool,
    )

    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[0:3] == ['opm', 'index', 'add']
    if bundles:
        assert ','.join(bundles) in opm_args
    else:
        assert '""' in opm_args
    if from_index:
        assert '--from-index' in opm_args
        assert from_index in opm_args
    else:
        assert '--from-index' not in opm_args
    if overwrite_csv:
        assert '--overwrite-latest' in opm_args
    else:
        assert '--overwrite-latest' not in opm_args
    if container_tool:
        assert '--container-tool' in opm_args
        assert container_tool in opm_args
    else:
        assert '--container-tool' not in opm_args
    if graph_update_mode:
        assert '--mode' in opm_args
        assert graph_update_mode in opm_args
    else:
        assert '--mode' not in opm_args
    assert "--enable-alpha" in opm_args

    mock_srt.assert_called_once_with('user:pass', from_index, append=True)


@pytest.mark.parametrize('container_tool', (None, 'podwoman'))
@mock.patch('iib.workers.tasks.utils.set_registry_token')
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_opm_index_rm(mock_run_cmd, mock_srt, container_tool):
    operators = ['operator_1', 'operator_2']
    opm_operations.opm_index_rm(
        '/tmp/somedir',
        operators,
        'binary-image:latest',
        'some_index:latest',
        'user:pass',
        container_tool=container_tool,
    )

    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[0:3] == ['opm', 'index', 'rm']
    assert ','.join(operators) in opm_args
    assert 'some_index:latest' in opm_args
    if container_tool:
        assert '--container-tool' in opm_args
        assert container_tool in opm_args
    else:
        assert '--container-tool' not in opm_args
    mock_srt.assert_called_once_with('user:pass', 'some_index:latest', append=True)


@pytest.mark.parametrize(
    'from_index, index_version',
    [('from_index@sha:415', 'v4.15'), ('from_index@sha:qeimage', 'v4.11')],
)
@mock.patch('iib.workers.tasks.utils.get_image_label')
def test_set_opm_version(mock_gil, from_index, index_version):
    mock_gil.return_value = index_version
    opm_operations.Opm.set_opm_version(from_index=from_index)
    assert (
        opm_operations.Opm.opm_version
        == get_worker_config().get('iib_ocp_opm_mapping')[index_version]
    )


@pytest.mark.parametrize(
    'from_index, index_version',
    [(None, None), ('from_index@sha:absentinconfig', 'v4.00')],
)
@mock.patch('iib.workers.tasks.utils.get_image_label')
def test_set_opm_version_default(mock_gil, from_index, index_version):
    mock_gil.return_value = index_version
    opm_operations.Opm.set_opm_version(from_index=from_index)
    assert opm_operations.Opm.opm_version == get_worker_config().get('iib_default_opm')
    if from_index is None:
        assert mock_gil.call_count == 0


@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_get_opm_version_number(mock_run_cmd):
    """Test opm version command result parsing."""
    mock_run_cmd.return_value = 'version.Version{OpmVersion:"v1.26.4", GoOs:"linux"}'
    assert Opm.get_opm_version_number() == '1.26.4'


@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_get_opm_version_number_fail(mock_run_cmd):
    """Test opm version command result parsing."""
    mock_run_cmd.return_value = 'OPM: command not found...'
    with pytest.raises(
        IIBError,
        match='Opm version not found in the output of \"OPM version\" command',
    ):
        Opm.get_opm_version_number()


@pytest.mark.parametrize(
    'input, is_input_dir, is_fbc',
    [
        ('/tmp/somedir', True, False),
        (
            'registry.example.com/example-rhel9:v4.17',
            False,
            False,
        ),
        (
            'registry.example.com/example-rhel9:v4.17',
            False,
            True,
        ),
    ],
)
@mock.patch('iib.workers.tasks.opm_operations.get_catalog_dir')
@mock.patch('iib.workers.tasks.build._get_index_database')
@mock.patch('iib.workers.tasks.opm_operations.is_image_fbc')
def test__get_input_data_path(mock_iif, mock_gid, mock_gcd, input, is_input_dir, is_fbc, tmpdir):
    mock_iif.return_value = is_fbc

    _get_input_data_path(input_image_or_path=input, base_dir=tmpdir)

    if is_input_dir:
        mock_iif.assert_called_once_with(input)
    else:
        mock_iif.assert_called_once_with(input)
        if is_fbc:
            mock_gcd.assert_called_once_with(input, tmpdir)
        else:
            mock_gid.assert_called_once_with(input, tmpdir)


@pytest.mark.parametrize(
    "bundle, version",
    [
        (
            {
                "csvName": "example.v2.0.8",
                "packageName": "example_operator",
                "channelName": "1.0",
                "bundlePath": "registry.example.com/example_operator@sha256:SHA-LONG_H3X_NUMBER",
                "providedApis": [
                    {
                        "group": "example_operator.io",
                        "version": "v1",
                        "kind": "ServiceMeshControlPlane",
                    },
                    {
                        "group": "example_operator.io",
                        "version": "v2",
                        "kind": "ServiceMeshControlPlane",
                    },
                    {"group": "example_operator.io", "version": "v1", "kind": "ServiceMeshMember"},
                    {
                        "group": "example_operator.io",
                        "version": "v1",
                        "kind": "ServiceMeshMemberRoll",
                    },
                ],
                "version": "2.0.8-0",
                "skipRange": ">=1.0.2 <2.0.8-0",
                "properties": [
                    {
                        "type": "olm.package",
                        "value": {'packageName': 'example_operator', 'version': '2.0.8-0'},
                    },
                ],
                "replaces": "example_operator.v2.0.7.1",
            },
            "2.0.8-0",
        )
    ],
)
def test__get_olm_package_version(bundle, version):
    ver = _get_olm_bundle_version(olm_bundle=bundle)
    assert ver == version


def test__get_olm_package_version_raise():
    bundle = {
        "csvName": "incorrect_budnle_data",
        "properties": [
            {
                "type": "olm.not_package",
            },
        ],
    }
    with pytest.raises(IIBError, match="No olm package version found for OLM bundle."):
        _get_olm_bundle_version(olm_bundle=bundle)


@mock.patch('iib.workers.tasks.opm_operations._get_input_data_path')
@mock.patch('iib.workers.tasks.utils.run_cmd')
@mock.patch.object(opm_operations.Opm, 'opm_version', 'opm-v1.26.8')
def test_get_list_bundles(mock_run_cmd, mock_gidp, tmpdir):
    input_image = 'registry.example.com/example_operator:tag'
    input_data_path = '/tmp/path'
    mock_gidp.return_value = input
    mock_gidp.return_value = input_data_path

    opm_render_output = """
    {
    "schema": "olm.package",
    "name": "example-operator",
    "defaultChannel": "stable"
}
{
    "schema": "olm.channel",
    "name": "stable",
    "package": "example-operator",
    "entries": [
        {
            "name": "example-operator.v0.1.0"
        }
    ]
}
{
    "schema": "olm.bundle",
    "name": "example-operator.v0.1.0",
    "package": "example-operator",
    "image": "quay.io/joelanford/example-operator-bundle:0.1.0",
    "properties": [
        {
            "type": "olm.package",
            "value": {
                "packageName": "example-operator",
                "version": "0.1.0"
            }
        }
    ],
    "relatedImages": [
        {
            "name": "",
            "image": "quay.io/joelanford/example-operator:0.1.0"
        }
    ]
}
{
    "schema": "olm.bundle",
    "name": "example-operator.v0.2.0",
    "package": "example-operator",
    "image": "quay.io/joelanford/example-operator-bundle:0.2.0",
    "properties": [
        {
            "type": "olm.package",
            "value": {
                "packageName": "example-operator",
                "version": "0.2.0"
            }
        }
    ],
    "relatedImages": [
        {
            "name": "",
            "image": "quay.io/joelanford/example-operator:0.2.0"
        }
    ]
}
    """

    mock_run_cmd.return_value = opm_render_output

    bundles = get_list_bundles(input_data=input_image, base_dir=tmpdir)

    assert bundles == [
        BundleImage(
            bundlePath='quay.io/joelanford/example-operator-bundle:0.1.0',
            csvName='example-operator.v0.1.0',
            packageName='example-operator',
            version='0.1.0',
        ),
        BundleImage(
            bundlePath='quay.io/joelanford/example-operator-bundle:0.2.0',
            csvName='example-operator.v0.2.0',
            packageName='example-operator',
            version='0.2.0',
        ),
    ]
    mock_run_cmd.assert_called_once_with(
        ['opm-v1.26.8', 'render', input_data_path],
        {'cwd': tmpdir},
        exc_msg=f'Failed to run opm render with input: {input_image}',
    )


@mock.patch('iib.workers.tasks.opm_operations._get_input_data_path')
@mock.patch('iib.workers.tasks.utils.run_cmd')
@mock.patch.object(opm_operations.Opm, 'opm_version', 'opm-v1.26.8')
def test_get_operator_package_list(mock_run_cmd, mock_gidp, tmpdir):
    input_image = 'registry.example.com/example_operator:tag'
    input_data_path = '/tmp/path'
    mock_gidp.return_value = input
    mock_gidp.return_value = input_data_path

    opm_render_output = """
    {
    "schema": "olm.package",
    "name": "example-operator",
    "defaultChannel": "stable"
}
{
    "schema": "olm.channel",
    "name": "stable",
    "package": "example-operator",
    "entries": [
        {
            "name": "example-operator.v0.1.0"
        }
    ]
}
{
    "schema": "olm.bundle",
    "name": "example-operator.v0.1.0",
    "package": "example-operator",
    "image": "quay.io/joelanford/example-operator-bundle:0.1.0",
    "properties": [
        {
            "type": "olm.package",
            "value": {
                "packageName": "example-operator",
                "version": "0.1.0"
            }
        }
    ],
    "relatedImages": [
        {
            "name": "",
            "image": "quay.io/joelanford/example-operator:0.1.0"
        }
    ]
}
    """

    mock_run_cmd.return_value = opm_render_output
    packages = get_operator_package_list(input_image_or_path=input_image, base_dir=tmpdir)

    assert packages == ['example-operator']
    mock_run_cmd.assert_called_once_with(
        ['opm-v1.26.8', 'render', input_data_path],
        {'cwd': tmpdir},
        exc_msg=f'Failed to run opm render with input: {input_image}',
    )
