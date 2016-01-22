INSERT_CUTSHEET_STG = """
INSERT INTO stg_cutsheet (`destination_data_center`, `move_date` , `relocation` , `move_number`,
    `asset_tag_num` , `serial_num` , `system_name`, `manufacturer`, `device_type`, `system_model_num`,
    `source_cabinet` , `total_rmu` , `destination_cabinet`, `team`, `support_owner` , `support_number` ,
    `low_u` , `high_u` , `src_power_src_circuit_num` , `src_ps1`, `src_ps2`, `src_ps3`, `src_ps4`,
    `dest_cab_recepticle_num`, `dest_ps1`, `dest_ps2`, `dest_ps3`, `dest_ps4`, `source_ip_address` ,
    `current_ip_address` , `destination_ip_address` , `server_port_id` , `switch_slot_port_id`,
    `media` , `current_switch_port_num`, `new_switch_port_num`, `critical_comments_notes`,
    `deinstaller_mover` , `deinstaller_mover_date` , `data_collector` , `data_collector_date`,
    `completed_by`, `completed_by_date` , `reviewer`, `reviewer_date`, `filename`)
VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}',
        '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}',
        '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')
"""

INSERT_TASK_LOG = """
INSERT INTO task_log (task_name, load_date, file_name, processed)
VALUES ('{}', {}, '{}', {})
"""
