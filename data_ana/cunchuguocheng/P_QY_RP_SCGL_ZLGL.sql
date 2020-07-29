
CREATE OR REPLACE
procedure P_QY_RP_SCGL_ZLGL
  ------------------------------------------------------------------------------
  ---�����������Ѳ��Ի��� ��������  rp���Ǩ�Ƶ���ʽ����rp��
  ---�����                                                                ---
  ---�����                                                                ---
  ---ʱ�䣺2019-07-02                                                          ---
  ---���ߣ�liuqingfeng                                                        ---
  ------------------------------------------------------------------------------
  (BEGINTIME IN VARCHAR2, O_SUCCESS OUT VARCHAR2, O_MSG OUT VARCHAR2) AS
  V_SUCCESSFLAG   CHAR;
BEGIN
  V_SUCCESSFLAG   := 'S';
  
  
    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_gndscfx';
    insert into rp_zlgl_hntfx_gndscfx
    select * from rp_zlgl_hntfx_gndscfx@保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_gndscfx_ejdw';
    insert into rp_zlgl_hntfx_gndscfx_ejdw
    select * from rp_zlgl_hntfx_gndscfx_ejdw@保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_kcj_ejdw';
    insert into rp_zlgl_hj_kcj_ejdw
    select * from rp_zlgl_hj_kcj_ejdw@保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_gdwscfx';
    insert into rp_zlgl_hntfx_gdwscfx
    select * from rp_zlgl_hntfx_gdwscfx@保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_jwj_ejdw';
    insert into rp_zlgl_hj_jwj_ejdw
    select * from rp_zlgl_hj_jwj_ejdw@保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_kcjmx';
    insert into rp_zlgl_hj_kcjmx
    select * from rp_zlgl_hj_kcjmx@保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_jjgfx_ejdwb';
    insert into rp_zlgl_jjgfx_ejdwb
    select * from rp_zlgl_jjgfx_ejdwb@保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_ejdw_cblfx';
    insert into rp_zlgl_hntfx_ejdw_cblfx
    select * from rp_zlgl_hntfx_ejdw_cblfx@保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_jungfx_jdjgqkb_ejdw';
    insert into rp_zlgl_jungfx_jdjgqkb_ejdw
    select * from rp_zlgl_jungfx_jdjgqkb_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_lbj';
    insert into rp_zlgl_hj_lbj
    select * from rp_zlgl_hj_lbj@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_lbjmx';
    insert into rp_zlgl_hj_lbjmx
    select * from rp_zlgl_hj_lbjmx@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_kcj';
    insert into rp_zlgl_hj_kcj
    select * from rp_zlgl_hj_kcj@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_sbjj_lx';
    insert into rp_zlgl_hj_sbjj_lx
    select * from rp_zlgl_hj_sbjj_lx@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_jwjmx';
    insert into rp_zlgl_hj_jwjmx
    select * from rp_zlgl_hj_jwjmx@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_hj';
    insert into rp_zlgl_hj_hj
    select * from rp_zlgl_hj_hj@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_ljxmmxb';
    insert into rp_zlgl_hntfx_ljxmmxb
    select * from rp_zlgl_hntfx_ljxmmxb@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_jungfx_xmlxfx';
    insert into rp_zlgl_jungfx_xmlxfx
    select * from rp_zlgl_jungfx_xmlxfx@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_jungfx_xmlxfx_ejdw';
    insert into rp_zlgl_jungfx_xmlxfx_ejdw
    select * from rp_zlgl_jungfx_xmlxfx_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_jxmxb';
    insert into rp_zlgl_hj_jxmxb
    select * from rp_zlgl_hj_jxmxb@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_zljmx';
    insert into rp_zlgl_hj_zljmx
    select * from rp_zlgl_hj_zljmx@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_gjyzgcj_ejdw';
    insert into rp_zlgl_hj_gjyzgcj_ejdw
    select * from rp_zlgl_hj_gjyzgcj_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_gydscfx_ejdw';
    insert into rp_zlgl_hntfx_gydscfx_ejdw
    select * from rp_zlgl_hntfx_gydscfx_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_jjgfx_mxb_ejdw';
    insert into rp_zlgl_jjgfx_mxb_ejdw
    select * from rp_zlgl_jjgfx_mxb_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_zljmx_ejdw';
    insert into rp_zlgl_hj_zljmx_ejdw
    select * from rp_zlgl_hj_zljmx_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_sbjj_lx_ejdw';
    insert into rp_zlgl_hj_sbjj_lx_ejdw
    select * from rp_zlgl_hj_sbjj_lx_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_jjgfx_mxb';
    insert into rp_zlgl_jjgfx_mxb
    select * from rp_zlgl_jjgfx_mxb@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_jwjmx_ejdw';
    insert into rp_zlgl_hj_jwjmx_ejdw
    select * from rp_zlgl_hj_jwjmx_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_jungfx_jdjgqkb';
    insert into rp_zlgl_jungfx_jdjgqkb
    select * from rp_zlgl_jungfx_jdjgqkb@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_bhjmxb_ejdw';
    insert into rp_zlgl_hntfx_bhjmxb_ejdw
    select * from rp_zlgl_hntfx_bhjmxb_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_zlj';
    insert into rp_zlgl_hj_zlj
    select * from rp_zlgl_hj_zlj@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_lbj_ejdw';
    insert into rp_zlgl_hj_lbj_ejdw
    select * from rp_zlgl_hj_lbj_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_jiaogfx_xmlxfx';
    insert into rp_zlgl_jiaogfx_xmlxfx
    select * from rp_zlgl_jiaogfx_xmlxfx@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_jjgfx_ztqkb_ejdw';
    insert into rp_zlgl_jjgfx_ztqkb_ejdw
    select * from rp_zlgl_jjgfx_ztqkb_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_cblfx_ejdw';
    insert into rp_zlgl_hntfx_cblfx_ejdw
    select * from rp_zlgl_hntfx_cblfx_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_kcjmx_ejdw';
    insert into rp_zlgl_hj_kcjmx_ejdw
    select * from rp_zlgl_hj_kcjmx_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_lbjmx_ejdw';
    insert into rp_zlgl_hj_lbjmx_ejdw
    select * from rp_zlgl_hj_lbjmx_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_sbjmx_ejdw';
    insert into rp_zlgl_hj_sbjmx_ejdw
    select * from rp_zlgl_hj_sbjmx_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_hj_ejdw';
    insert into rp_zlgl_hj_hj_ejdw
    select * from rp_zlgl_hj_hj_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_xmmxb';
    insert into rp_zlgl_hntfx_xmmxb
    select * from rp_zlgl_hntfx_xmmxb@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_jwj';
    insert into rp_zlgl_hj_jwj
    select * from rp_zlgl_hj_jwj@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_sbjj_ztqk_ejdw';
    insert into rp_zlgl_hj_sbjj_ztqk_ejdw
    select * from rp_zlgl_hj_sbjj_ztqk_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_bhjmxb';
    insert into rp_zlgl_hntfx_bhjmxb
    select * from rp_zlgl_hntfx_bhjmxb@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_jiaogfx_jdjgqkb_ejdw';
    insert into rp_zlgl_jiaogfx_jdjgqkb_ejdw
    select * from rp_zlgl_jiaogfx_jdjgqkb_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_scqkb';
    insert into rp_zlgl_hntfx_scqkb
    select * from rp_zlgl_hntfx_scqkb@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_jiaogfx_xmlxfx_ejdw';
    insert into rp_zlgl_jiaogfx_xmlxfx_ejdw
    select * from rp_zlgl_jiaogfx_xmlxfx_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_gjyzzgcjmx_ejdw';
    insert into rp_zlgl_hj_gjyzzgcjmx_ejdw
    select * from rp_zlgl_hj_gjyzzgcjmx_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_scqkb_ejdw';
    insert into rp_zlgl_hntfx_scqkb_ejdw
    select * from rp_zlgl_hntfx_scqkb_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_sbjj_ztqk';
    insert into rp_zlgl_hj_sbjj_ztqk
    select * from rp_zlgl_hj_sbjj_ztqk@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_zlj_ejdw';
    insert into rp_zlgl_hj_zlj_ejdw
    select * from rp_zlgl_hj_zlj_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_jjgfx_ztqkb';
    insert into rp_zlgl_jjgfx_ztqkb
    select * from rp_zlgl_jjgfx_ztqkb@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_jiaogfx_jdjgqkb';
    insert into rp_zlgl_jiaogfx_jdjgqkb
    select * from rp_zlgl_jiaogfx_jdjgqkb@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_yljmxb';
    insert into rp_zlgl_hntfx_yljmxb
    select * from rp_zlgl_hntfx_yljmxb@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_cblfx';
    insert into rp_zlgl_hntfx_cblfx
    select * from rp_zlgl_hntfx_cblfx@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_gjyzgcj';
    insert into rp_zlgl_hj_gjyzgcj
    select * from rp_zlgl_hj_gjyzgcj@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_sbjmx';
    insert into rp_zlgl_hj_sbjmx
    select * from rp_zlgl_hj_sbjmx@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hj_gjyzzgcjmx';
    insert into rp_zlgl_hj_gjyzzgcjmx
    select * from rp_zlgl_hj_gjyzzgcjmx@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_yljmxb_ejdw';
    insert into rp_zlgl_hntfx_yljmxb_ejdw
    select * from rp_zlgl_hntfx_yljmxb_ejdw@DBLINK_RP_保密;
    commit;


    execute immediate 'TRUNCATE TABLE rp_zlgl_hntfx_gydscfx';
    insert into rp_zlgl_hntfx_gydscfx
    select * from rp_zlgl_hntfx_gydscfx@DBLINK_RP_保密;
    commit;




  if v_successflag <> 'F' then
    o_success := 'S';
    o_msg     := '�ɹ�ת����ϣ�����';
  else
    o_msg := '���ֻ�ȫ����δ�������룬��ϸ��Ϣ����־��¼';
  end if;

  --�������֡�OTHERS��ʾ������������������SQLERRM��ϵͳ���ñ��������˵�ǰ�������ϸ��Ϣ��
exception
  when others then
    rollback;
    --�ѵ�ǰ�����¼����־��
    o_success := 'F';
    o_msg     := 'ʧ�ܣ�' || sqlerrm;

    commit;

end P_QY_RP_SCGL_ZLGL;

