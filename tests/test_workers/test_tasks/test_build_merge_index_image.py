# SPDX-License-Identifier: GPL-3.0-or-later
import json
import os
import stat
from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import build_merge_index_image
from iib.workers.tasks.utils import RequestConfigMerge


@pytest.mark.parametrize('source_fbc, target_fbc', [(False, False), (False, True), (True, True)])
@pytest.mark.parametrize(
    'target_index, target_index_resolved, binary_image',
    (
        ('target-from-index:1.0', 'target-index@sha256:resolved', 'binary-image:1.0'),
        (None, None, None),
    ),
)
@mock.patch('iib.workers.tasks.opm_operations._get_input_data_path')
@mock.patch('iib.workers.tasks.utils.run_cmd')
@mock.patch('iib.workers.tasks.build_merge_index_image._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build._verify_index_image')
@mock.patch('iib.workers.tasks.build_merge_index_image.create_dockerfile')
@mock.patch('iib.workers.tasks.build._get_index_database')
@mock.patch('iib.workers.tasks.build_merge_index_image.opm_migrate')
@mock.patch('iib.workers.tasks.build_merge_index_image._push_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._build_image')
@mock.patch('iib.workers.tasks.build_merge_index_image.deprecate_bundles_fbc')
@mock.patch('iib.workers.tasks.build_merge_index_image.verify_operators_exists')
@mock.patch('iib.workers.tasks.build_merge_index_image.deprecate_bundles')
@mock.patch('iib.workers.tasks.build_merge_index_image._get_external_arch_pull_spec')
@mock.patch('iib.workers.tasks.build_merge_index_image.get_bundles_from_deprecation_list')
@mock.patch(
    'iib.workers.tasks.build_merge_index_image._add_bundles_missing_in_source',
    return_value=([{'bundlePath': 'some_bundle'}], []),
)
@mock.patch('iib.workers.tasks.build_merge_index_image._get_present_bundles', return_value=[[], []])
@mock.patch('iib.workers.tasks.build_merge_index_image.set_request_state')
@mock.patch('iib.workers.tasks.build_merge_index_image._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_merge_index_image.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_merge_index_image._cleanup')
@mock.patch('iib.workers.tasks.build_merge_index_image._add_label_to_index')
@mock.patch('iib.workers.tasks.build_merge_index_image.set_registry_token')
@mock.patch('subprocess.run')
@mock.patch('iib.workers.tasks.build_merge_index_image.is_image_fbc')
@mock.patch('iib.workers.tasks.build.get_worker_config')
@mock.patch('iib.workers.tasks.opm_operations.Opm.set_opm_version')
def test_handle_merge_request(
    mock_sov,
    mock_gwc,
    mock_iifbc,
    mock_run,
    mock_set_registry_token,
    mock_add_label_to_index,
    mock_cleanup,
    mock_prfb,
    mock_uiibs,
    mock_srs,
    mock_gpb,
    mock_abmis,
    mock_gbfdl,
    mock_geaps,
    mock_dep_b,
    mock_verify_operator_exits,
    mock_dep_b_fbc,
    mock_bi,
    mock_pi,
    mock_om,
    mock_gid,
    mock_ogd,
    mock_vii,
    mock_uiips,
    mock_run_cmd,
    mock_gidp,
    target_index,
    target_index_resolved,
    binary_image,
    source_fbc,
    target_fbc,
):
    mock_run.return_value.returncode = 0
    prebuild_info = {
        'arches': {'amd64', 'other_arch'},
        'binary_image': binary_image,
        'target_ocp_version': '4.6',
        'source_from_index_resolved': 'source-index@sha256:resolved',
        'target_index_resolved': target_index_resolved,
        'distribution_scope': 'stage',
    }

    # returns different values to test different cases
    # depends on sequence of is_index_fbc calls:
    # 1st - source_fbc = is_image_fbc(source_from_index_resolved)
    # 2nd - target_fbc = is_image_fbc(target_index_resolved)
    # 3rd - is_source_fbc = is_image_fbc(source_from_index)
    mock_iifbc.side_effect = [source_fbc, target_fbc, source_fbc]

    mock_prfb.return_value = prebuild_info
    mock_gbfdl.return_value = ['some-bundle:1.0']
    binary_image_config = {'prod': {'v4.5': 'some_image'}, 'stage': {'stage': 'some_other_img'}}
    mock_gwc.return_value = {
        'iib_registry': 'quay.io',
        'iib_image_push_template': '{registry}/iib-build:{request_id}',
        'iib_api_url': 'http://iib-api:8080/api/v1/',
    }
    mock_gid.return_value = 'database/index.db'
    mock_om.return_value = 'catalog', 'cache'

    # Simulate opm's behavior of creating files that cannot be deleted
    def side_effect(*args, base_dir, **kwargs):
        read_only_dir = os.path.join(base_dir, 'read-only-dir')
        os.mkdir(read_only_dir)
        with open(os.path.join(read_only_dir, 'read-only-file'), 'w') as f:
            os.chmod(f.fileno(), stat.S_IRUSR | stat.S_IRGRP)
        # Make the dir read-only *after* populating it
        os.chmod(read_only_dir, mode=stat.S_IRUSR | stat.S_IRGRP)

    mock_dep_b.side_effect = side_effect
    mock_dep_b_fbc.side_effect = side_effect
    mock_verify_operator_exits.return_value = (mock_dep_b, "")

    mock_gidp.return_value = '/tmp'
    mock_run_cmd.return_value = json.dumps(
        {
            "schema": "olm.bundle",
            "image": "bundle1",
            "name": "name1",
            "package": "package1",
            "version": "v1.0",
            "properties": [{"type": "olm.package", "value": {"version": "0.1.0"}}],
        }
    )

    build_merge_index_image.handle_merge_request(
        'source-from-index:1.0',
        ['some-bundle:1.0'],
        1,
        binary_image,
        target_index,
        distribution_scope='stage',
        binary_image_config=binary_image_config,
    )

    if target_fbc:
        mock_om.assert_called_once()

    assert mock_cleanup.call_count == 2
    mock_prfb.assert_called_once_with(
        1,
        RequestConfigMerge(
            _binary_image=binary_image,
            overwrite_target_index_token=None,
            source_from_index='source-from-index:1.0',
            target_index=target_index,
            distribution_scope='stage',
            binary_image_config=binary_image_config,
        ),
    )
    mock_uiibs.assert_called_once_with(1, prebuild_info)
    if target_index:
        assert mock_gpb.call_count == 2
    else:
        assert mock_gpb.call_count == 1
    mock_vii.assert_not_called()
    mock_abmis.assert_called_once()
    mock_gbfdl.assert_called_once()
    mock_geaps.assert_called_once()
    if source_fbc:
        mock_dep_b_fbc.assert_called_once()
        assert mock_bi.call_count == 2
        assert mock_pi.call_count == 2
    else:
        mock_dep_b.assert_called_once()
        assert mock_bi.call_count == 3
        assert mock_pi.call_count == 3
    mock_set_registry_token.call_count == 2
    assert mock_add_label_to_index.call_count == 2
    mock_uiips.assert_called_once()

    mock_sov.assert_called_once_with(target_index_resolved)
    mock_run_cmd.assert_called_once()


@pytest.mark.parametrize('source_fbc, target_fbc', [(False, False), (False, True), (True, True)])
@pytest.mark.parametrize(
    'invalid_bundles, filtered_invalid_version_bundles_names',
    [
        ([], {}),
        (
            [{'bundlePath': 'invalid_bundle:1.0', "packageName": "invalid_bundle"}],
            {'invalid_bundle'},
        ),
        ([{'bundlePath': 'invalid_bundle:1.0', "packageName": "invalid_bundle"}], {}),
    ],
)
@mock.patch('iib.workers.tasks.opm_operations._get_input_data_path')
@mock.patch('iib.workers.tasks.utils.run_cmd')
@mock.patch('iib.workers.tasks.build_merge_index_image._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build._verify_index_image')
@mock.patch('iib.workers.tasks.build_merge_index_image.create_dockerfile')
@mock.patch('iib.workers.tasks.build._get_index_database')
@mock.patch('iib.workers.tasks.build_merge_index_image.opm_migrate')
@mock.patch('iib.workers.tasks.build_merge_index_image._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build_merge_index_image._push_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._build_image')
@mock.patch('iib.workers.tasks.build_merge_index_image.deprecate_bundles_fbc')
@mock.patch('iib.workers.tasks.build_merge_index_image.verify_operators_exists')
@mock.patch('iib.workers.tasks.build_merge_index_image.deprecate_bundles')
@mock.patch('iib.workers.tasks.build_merge_index_image._get_external_arch_pull_spec')
@mock.patch('iib.workers.tasks.build_merge_index_image.get_bundles_from_deprecation_list')
@mock.patch('iib.workers.tasks.build_merge_index_image._add_bundles_missing_in_source')
@mock.patch(
    'iib.workers.tasks.build_merge_index_image._get_present_bundles',
    return_value=[[{'bundlePath': 'some_bundle'}], []],
)
@mock.patch('iib.workers.tasks.utils.set_request_state')
@mock.patch('iib.workers.tasks.build_merge_index_image.set_request_state')
@mock.patch('iib.workers.tasks.build_merge_index_image._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_merge_index_image.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_merge_index_image._cleanup')
@mock.patch('iib.workers.tasks.build_merge_index_image._add_label_to_index')
@mock.patch('iib.workers.tasks.build_merge_index_image.is_image_fbc')
@mock.patch('iib.workers.tasks.opm_operations.Opm.set_opm_version')
@mock.patch('iib.workers.tasks.opm_operations.get_hidden_index_database')
def test_handle_merge_request_no_deprecate(
    mock_get_hidden_db,
    mock_sov,
    mock_iifbc,
    mock_add_label_to_index,
    mock_cleanup,
    mock_prfb,
    mock_uiibs,
    mock_srs,
    mock_srs2,
    mock_gpb,
    mock_abmis,
    mock_gbfdl,
    mock_geaps,
    mock_dep_b,
    mock_verify_operators_exists,
    mock_dep_b_fbc,
    mock_bi,
    mock_pi,
    mock_capml,
    mock_om,
    mock_gid,
    mock_ogd,
    mock_vii,
    mock_uiips,
    mock_run_cmd,
    mock_gidp,
    invalid_bundles,
    filtered_invalid_version_bundles_names,
    source_fbc,
    target_fbc,
):
    target_index_resolved = 'target-index@sha256:resolved'
    prebuild_info = {
        'arches': {'amd64', 'other_arch'},
        'binary_image': 'binary-image:1.0',
        'target_ocp_version': '4.6',
        'source_from_index_resolved': 'source-index@sha256:resolved',
        'target_index_resolved': target_index_resolved,
        'distribution_scope': 'stage',
    }

    # returns different values to test different cases
    # depends on sequence of is_index_fbc calls:
    # 1st - source_fbc = is_image_fbc(source_from_index_resolved)
    # 2nd - target_fbc = is_image_fbc(target_index_resolved)
    # 3rd - is_source_fbc = is_image_fbc(source_from_index)
    mock_iifbc.side_effect = [source_fbc, target_fbc, source_fbc]

    mock_prfb.return_value = prebuild_info
    mock_gbfdl.return_value = []
    mock_abmis.return_value = ([], invalid_bundles)
    mock_gid.return_value = 'database/index.db'
    mock_om.return_value = 'catalog', 'cache'
    mock_run_cmd.return_value = json.dumps(
        {
            "schema": "olm.bundle",
            "image": "bundle1",
            "name": "name1",
            "package": "package1",
            "version": "v1.0",
            "properties": [{"type": "olm.package", "value": {"version": "0.1.0"}}],
        }
    )
    mock_verify_operators_exists.return_value = (filtered_invalid_version_bundles_names, "db_path")
    build_merge_index_image.handle_merge_request(
        'source-from-index:1.0',
        ['some-bundle:1.0'],
        1,
        'binary-image:1.0',
        'target-from-index:1.0',
        distribution_scope='stage',
    )

    if target_fbc:
        mock_om.assert_called_once()

    assert mock_cleanup.call_count == 2
    mock_prfb.assert_called_once_with(
        1,
        RequestConfigMerge(
            _binary_image='binary-image:1.0',
            binary_image_config=None,
            overwrite_target_index_token=None,
            source_from_index='source-from-index:1.0',
            target_index='target-from-index:1.0',
            distribution_scope='stage',
        ),
    )
    mock_uiibs.assert_called_once_with(1, prebuild_info)
    assert mock_gpb.call_count == 2
    mock_abmis.assert_called_once()
    mock_gbfdl.assert_called_once()
    mock_geaps.assert_called_once()
    if invalid_bundles:
        if filtered_invalid_version_bundles_names:
            mock_verify_operators_exists.assert_called_once()

            if source_fbc:
                mock_dep_b_fbc.assert_called_once_with(
                    bundles=['invalid_bundle:1.0'],
                    base_dir=mock.ANY,
                    binary_image='binary-image:1.0',
                    from_index=mock.ANY,
                )
                assert mock_bi.call_count == 2
                assert mock_pi.call_count == 2
            else:
                mock_dep_b.assert_called_once_with(
                    bundles=['invalid_bundle:1.0'],
                    base_dir=mock.ANY,
                    binary_image='binary-image:1.0',
                    from_index=mock.ANY,
                    overwrite_target_index_token=None,
                )
                assert mock_bi.call_count == 3
                assert mock_pi.call_count == 3
        else:
            mock_dep_b_fbc.assert_not_called()
    else:
        mock_dep_b.assert_not_called()
        mock_dep_b_fbc.assert_not_called()
        assert mock_bi.call_count == 2
        assert mock_pi.call_count == 2
    assert mock_add_label_to_index.call_count == 2
    mock_vii.assert_not_called()
    mock_capml.assert_called_once_with(1, {'amd64', 'other_arch'}, None)
    mock_sov.assert_called_once_with(target_index_resolved)
    mock_uiips.assert_called_once()
    mock_run_cmd.assert_not_called()


@mock.patch('iib.workers.config.get_worker_config')
@mock.patch('iib.workers.tasks.build_merge_index_image.is_image_fbc')
@mock.patch('iib.workers.tasks.build_merge_index_image.get_image_label')
@mock.patch('iib.workers.tasks.build_merge_index_image._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build_merge_index_image._push_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._build_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._add_label_to_index')
@mock.patch('iib.workers.tasks.build_merge_index_image.opm_index_add')
@mock.patch('iib.workers.tasks.build_merge_index_image.set_request_state')
def test_add_bundles_missing_in_source(
    mock_srs, mock_oia, mock_aolti, mock_bi, mock_pi, mock_capml, mock_gil, mock_iifbc, mock_gwc
):
    source_bundles = [
        {
            'packageName': 'bundle1',
            'version': '1.0',
            'bundlePath': 'quay.io/bundle1@sha256:123456',
            'csvName': 'bundle1-1.0',
        },
        {
            'packageName': 'bundle2',
            'version': '2.0',
            'bundlePath': 'quay.io/bundle2@sha256:234567',
            'csvName': 'bundle2-2.0',
        },
        {
            'packageName': 'bundle5',
            'version': '5.0-2',
            'bundlePath': 'quay.io/bundle2@sha256:456132',
            'csvName': 'bundle5-5.0',
        },
    ]
    target_bundles = [
        {
            'packageName': 'bundle1',
            'version': '1.0',
            'bundlePath': 'quay.io/bundle1@sha256:123456',
            'csvName': 'bundle1-1.0',
        },
        {
            'packageName': 'bundle3',
            'version': '3.0',
            'bundlePath': 'quay.io/bundle3@sha256:456789',
            'csvName': 'bundle3-3.0',
        },
        {
            'packageName': 'bundle4',
            'version': '4.0',
            'bundlePath': 'quay.io/bundle4@sha256:567890',
            'csvName': 'bundle4-4.0',
        },
        {
            'packageName': 'bundle5',
            'version': '5.0-1',
            'bundlePath': 'quay.io/bundle4@sha256:569854',
            'csvName': 'bundle5-5.0',
        },
        {
            'packageName': 'bundle6-ignore-ocp-failed',
            'version': '14.0',
            'bundlePath': 'quay.io/ignore-ocp-failed-bundle4@sha256:567890',
            'csvName': 'bundle6-ignore-ocp-failed-14.0',
        },
    ]

    mock_gwc.iib_api_url.return_value = {
        'iib_no_ocp_label_allow_list': ['quay.io/bundle'],
    }

    mock_gil.side_effect = ['=v4.5', '=v4.6', 'v4.7', 'v4.5-v4.7', 'v4.5,v4.6', '']
    mock_iifbc.return_value = False
    missing_bundles, invalid_bundles = build_merge_index_image._add_bundles_missing_in_source(
        source_bundles,
        target_bundles,
        'some_dir',
        'binary-image:4.5',
        'index-image:4.6',
        1,
        'amd64',
        '4.6',
        'dev',
        'replaces',
        ignore_bundle_ocp_version=True,
    )
    assert missing_bundles == [
        {
            'packageName': 'bundle3',
            'version': '3.0',
            'bundlePath': 'quay.io/bundle3@sha256:456789',
            'csvName': 'bundle3-3.0',
        },
        {
            'packageName': 'bundle4',
            'version': '4.0',
            'bundlePath': 'quay.io/bundle4@sha256:567890',
            'csvName': 'bundle4-4.0',
        },
        {
            'bundlePath': 'quay.io/ignore-ocp-failed-bundle4@sha256:567890',
            'csvName': 'bundle6-ignore-ocp-failed-14.0',
            'packageName': 'bundle6-ignore-ocp-failed',
            'version': '14.0',
        },
    ]
    assert invalid_bundles == [
        {
            'packageName': 'bundle3',
            'version': '3.0',
            'bundlePath': 'quay.io/bundle3@sha256:456789',
            'csvName': 'bundle3-3.0',
        },
        {
            'packageName': 'bundle6-ignore-ocp-failed',
            'version': '14.0',
            'bundlePath': 'quay.io/ignore-ocp-failed-bundle4@sha256:567890',
            'csvName': 'bundle6-ignore-ocp-failed-14.0',
        },
        {
            'packageName': 'bundle5',
            'version': '5.0-2',
            'bundlePath': 'quay.io/bundle2@sha256:456132',
            'csvName': 'bundle5-5.0',
        },
    ]
    mock_srs.assert_called_once()
    mock_oia.assert_called_once_with(
        base_dir='some_dir',
        bundles=[
            'quay.io/bundle3@sha256:456789',
            'quay.io/bundle4@sha256:567890',
            'quay.io/ignore-ocp-failed-bundle4@sha256:567890',
        ],
        binary_image='binary-image:4.5',
        from_index='index-image:4.6',
        container_tool='podman',
        graph_update_mode='replaces',
    )
    assert mock_gil.call_count == 6
    assert mock_aolti.call_count == 2
    mock_bi.assert_called_once()
    mock_pi.assert_called_once()
    mock_capml.assert_not_called()


@pytest.mark.parametrize(
    'source_bundles, target_bundles, error_msg',
    (
        (
            [
                {
                    'packageName': 'bundle1',
                    'version': '1.0',
                    'bundlePath': 'quay.io/bundle1@sha256:123456',
                    'csvName': 'bundle1-1.0',
                },
                {
                    'packageName': 'bundle2',
                    'version': '2.0',
                    'bundlePath': 'quay.io/bundle2:234567',
                    'csvName': 'bundle2-2.0',
                },
            ],
            [
                {
                    'packageName': 'bundle1',
                    'version': '1.0',
                    'bundlePath': 'quay.io/bundle1@sha256:123456',
                },
                {
                    'packageName': 'bundle3',
                    'version': '3.0',
                    'bundlePath': 'quay.io/bundle3@sha256:456789',
                },
                {
                    'packageName': 'bundle4',
                    'version': '4.0',
                    'bundlePath': 'quay.io/bundle4@sha256:567890',
                },
            ],
            'Bundle quay.io/bundle2:234567 in the source index image is not defined via digest',
        ),
        (
            [
                {
                    'packageName': 'bundle1',
                    'version': '1.0',
                    'bundlePath': 'quay.io/bundle1@sha256:123456',
                    'csvName': 'bundle1-1.0',
                },
                {
                    'packageName': 'bundle2',
                    'version': '2.0',
                    'bundlePath': 'quay.io/bundle2:234567',
                    'csvName': 'bundle2-2.0',
                },
            ],
            [
                {
                    'packageName': 'bundle1',
                    'version': '1.0',
                    'bundlePath': 'quay.io/bundle1@sha256:123456',
                },
                {
                    'packageName': 'bundle3',
                    'version': '3.0',
                    'bundlePath': 'quay.io/bundle3@sha256:456789',
                },
                {
                    'packageName': 'bundle4',
                    'version': '4.0',
                    'bundlePath': 'quay.io/bundle4@sha256:567890',
                },
            ],
            'Bundle quay.io/bundle2:234567 in the source index image is not defined via digest',
        ),
    ),
)
@mock.patch('iib.workers.tasks.build_merge_index_image._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build_merge_index_image._push_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._build_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._add_label_to_index')
@mock.patch('iib.workers.tasks.build_merge_index_image.opm_index_add')
@mock.patch('iib.workers.tasks.build_merge_index_image.set_request_state')
def test_add_bundles_missing_in_source_error_tag_specified(
    mock_srs,
    mock_oia,
    mock_aolti,
    mock_bi,
    mock_pi,
    mock_capml,
    source_bundles,
    target_bundles,
    error_msg,
):
    with pytest.raises(IIBError, match=error_msg):
        build_merge_index_image._add_bundles_missing_in_source(
            source_bundles,
            target_bundles,
            'some_dir',
            'binary-image:4.5',
            'index-image:4.6',
            1,
            'amd64',
            '4.6',
            'dev',
            'semver',
        )


@mock.patch('iib.workers.tasks.build_merge_index_image.is_image_fbc')
@mock.patch('iib.workers.tasks.build_merge_index_image.get_image_label')
@mock.patch('iib.workers.tasks.build_merge_index_image._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build_merge_index_image._push_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._build_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._add_label_to_index')
@mock.patch('iib.workers.tasks.build_merge_index_image.opm_index_add')
@mock.patch('iib.workers.tasks.build_merge_index_image.set_request_state')
def test_add_bundles_missing_in_source_none_missing(
    mock_srs, mock_oia, mock_aolti, mock_bi, mock_pi, mock_capml, mock_gil, mock_iifbc
):
    source_bundles = [
        {
            'packageName': 'bundle1',
            'version': '1.0',
            'bundlePath': 'quay.io/bundle1@sha256:123456',
            'csvName': 'bundle1-1.0',
        },
        {
            'packageName': 'bundle2',
            'version': '2.0',
            'bundlePath': 'quay.io/bundle2@sha256:123456',
            'csvName': 'bundle2-2.0',
        },
        {
            'packageName': 'bundle3',
            'version': '3.0',
            'bundlePath': 'quay.io/bundle3@sha256:123456',
            'csvName': 'bundle3-3.0',
        },
        {
            'packageName': 'bundle4',
            'version': '4.0',
            'bundlePath': 'quay.io/bundle4@sha256:123456',
            'csvName': 'bundle4-4.0',
        },
    ]
    target_bundles = [
        {
            'packageName': 'bundle1',
            'version': '1.0',
            'bundlePath': 'quay.io/bundle1@sha256:123456',
            'csvName': 'bundle1-1.0',
        },
        {
            'packageName': 'bundle2',
            'version': '2.0',
            'bundlePath': 'quay.io/bundle2@sha256:123456',
            'csvName': 'bundle2-2.0',
        },
    ]
    mock_gil.side_effect = ['v=4.5', 'v4.8,v4.7', 'v4.5-v4.8', 'v4.5,v4.6,v4.7']
    mock_iifbc.return_value = False
    missing_bundles, invalid_bundles = build_merge_index_image._add_bundles_missing_in_source(
        source_bundles,
        target_bundles,
        'some_dir',
        'binary-image:4.5',
        'index-image:4.6',
        1,
        'amd64',
        '4.6',
        'dev',
        'semver',
    )
    assert missing_bundles == []
    assert invalid_bundles == [
        {
            'packageName': 'bundle1',
            'version': '1.0',
            'bundlePath': 'quay.io/bundle1@sha256:123456',
            'csvName': 'bundle1-1.0',
        },
        {
            'packageName': 'bundle2',
            'version': '2.0',
            'bundlePath': 'quay.io/bundle2@sha256:123456',
            'csvName': 'bundle2-2.0',
        },
    ]
    mock_srs.assert_called_once()
    mock_oia.assert_called_once_with(
        base_dir='some_dir',
        bundles=[],
        binary_image='binary-image:4.5',
        from_index='index-image:4.6',
        container_tool='podman',
        graph_update_mode='semver',
    )
    assert mock_gil.call_count == 4
    assert mock_aolti.call_count == 2
    mock_bi.assert_called_once()
    mock_pi.assert_called_once()
    mock_capml.assert_not_called()


@pytest.mark.parametrize(
    'version_label, ocp_version, allow_no_ocp_version, result',
    (
        ('=v4.5', 'v4.6', False, False),
        ('v4.5-v4.7', 'v4.6', False, True),
        ('=v4.9|=v4.10|>=v4.12', 'v4.8', False, False),
        ('=v4.9|=v4.10|>=v4.12', 'v4.9', False, True),
        ('=v4.9|=v4.10|>=v4.12', 'v4.10', False, True),
        ('=v4.9|=v4.10|>=v4.12', 'v4.11', False, False),
        ('=v4.9|=v4.10|>=v4.12', 'v4.12', False, True),
        ('=v4.9|=v4.10|>=v4.13', 'v4.13', False, True),
        ('=v4.5|=v4.6', 'v4.6', False, True),
        ('=v4.5|v4.10', 'v4.12', False, False),
        ('v4.5|>=v4.10', 'v4.12', False, False),
        ('>=v4.10', 'v4.6', False, False),
        ('>=v4.10', 'v4.10', False, False),
        ('v4.5-v4.7', 'v4.8', False, False),
        ('v4.6', 'v4.6', False, True),
        ('v=4.6', 'v4.6', False, False),
        ('v4.5,v4.6', 'v4.6', False, True),
        ('v4.6,v4.5', 'v4.10', False, True),
        ('tom_brady', 'v4.6', False, False),
        ('', 'v4.6', False, False),
        ('', 'v4.6', True, True),
    ),
)
@mock.patch('iib.workers.tasks.build_merge_index_image.get_image_label')
def test_is_bundle_version_valid(
    mock_gil, version_label, ocp_version, allow_no_ocp_version, result
):
    mock_gil.return_value = version_label
    is_valid = build_merge_index_image.is_bundle_version_valid(
        'some_bundle', ocp_version, allow_no_ocp_version
    )
    assert is_valid == result


# version_label is the target version of the index image. It should only ever be a single
# version in the format vX.Y where X and Y are both integers.
@pytest.mark.parametrize('version_label', ('random-version', 'v4.6,v4.5', 'v4.5,v4.6'))
def test_is_bundle_version_valid_invalid_index_ocp_version(version_label):
    match_str = f'Invalid OCP version, "{version_label}", specified in Index Image'
    with pytest.raises(IIBError, match=match_str):
        build_merge_index_image.is_bundle_version_valid('some_bundle', version_label, False)


@mock.patch('iib.workers.tasks.build_merge_index_image._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_merge_index_image.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_merge_index_image.set_request_state')
@mock.patch('iib.workers.config.get_worker_config')
@mock.patch('iib.workers.tasks.build_merge_index_image._cleanup')
@mock.patch('iib.workers.tasks.build_merge_index_image.is_image_fbc')
@mock.patch('iib.workers.tasks.opm_operations.Opm.set_opm_version')
def test_handle_merge_request_raises(
    mock_sov, mock_iifbc, mock_c, mock_gwc, mock_srs, mock_prfb, mock_uiibs
):
    # set true for source_fbc; false for target_fbc
    mock_iifbc.side_effect = (True, False)

    prebuild_info = {
        'arches': {'amd64', 'other_arch'},
        'binary_image': 'binary_image',
        'binary_image_resolved': 'binary_image_resolved',
        'target_ocp_version': '4.6',
        'source_from_index_resolved': 'source-index@sha256:resolved',
        'target_index_resolved': 'target_index_resolved',
        'distribution_scope': 'stage',
    }
    mock_prfb.return_value = prebuild_info

    mock_gwc.iib_api_url.return_value = {
        'iib_api_url': 'http://iib-api:8080/api/v1/',
    }
    with pytest.raises(
        IIBError,
        match="Cannot merge source File-Based Catalog index image into target SQLite index image.",
    ):
        build_merge_index_image.handle_merge_request(
            source_from_index='source-from-index:1.0',
            deprecation_list=['some-bundle:1.0'],
            request_id=1,
            binary_image='binary-image:1.0',
            target_index='target-from-index:1.0',
            distribution_scope='stage',
            binary_image_config={
                'prod': {'v4.5': 'some_image'},
                'stage': {'stage': 'some_other_img'},
            },
        )

        mock_uiibs.assert_called_once_with(1, prebuild_info)
