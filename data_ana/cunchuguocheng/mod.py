arl_del='''
CREATE OR REPLACE
procedure %(stoge_name)s
  ------------------------------------------------------------------------------
  ---功能描述：把测试环境 %(yewu)s  rp层表迁移到正式环境rp层
  ---输入表：                                                                ---
  ---输出表：                                                                ---
  ---时间：%(date)s                                                          ---
  ---作者：%(zuozhe)s                                                        ---
  ------------------------------------------------------------------------------
  (BEGINTIME IN VARCHAR2, O_SUCCESS OUT VARCHAR2, O_MSG OUT VARCHAR2) AS
  V_SUCCESSFLAG   CHAR;
BEGIN
  V_SUCCESSFLAG   := 'S';
  
  %(chongfusql)s


  if v_successflag <> 'F' then
    o_success := 'S';
    o_msg     := '成功转换完毕！！！';
  else
    o_msg := '部分或全部表未正常导入，详细信息见日志记录';
  end if;

  --错误处理部分。OTHERS表示除了声明外的任意错误。SQLERRM是系统内置变量保存了当前错误的详细信息。
exception
  when others then
    rollback;
    --把当前错误记录进日志表。
    o_success := 'F';
    o_msg     := '失败，' || sqlerrm;

    commit;

end %(stoge_name)s;
'''

sql_mod='''
    execute immediate 'TRUNCATE TABLE %(mubiao)s';
    insert into %(mubiao)s
    select * from %(yuanbiao)s@%(dizhi)s;
    commit;
'''
if __name__=='__main__':
    print('11111111111111111111111')
    import time
    dt = time.strftime('%Y-%m-%d', time.localtime())
    print(arl_del %{'stoge_name':'P_QY_RP_CW','mubiao':'zb_cbtz_zhzbqkb','date':dt,
                    'zuozhe':'liuqingfeng','yuanbiao':'zb_cbtz_zhzbqkb'}
          )