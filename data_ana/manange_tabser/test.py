import os
import tableauserverclient as TSC
import random
import string
from sqlalchemy import create_engine
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
engine = create_engine(
    'oracle+cx_oracle://ODS_保密:ODS_保密123@IP保密:1527/ORCL')
tableau_auth = TSC.TableauAuth('tableau账号','tableau密码')
server = TSC.Server('http://10.1.9.47:8081')

with server.auth.sign_in(tableau_auth):
    # all_datasources, pagination_item = server.datasources.get()
    server.version = '3.2'
    all_users, pagination_item = server.users.get()
    for user in all_users:
        if user.site_role == 'Viewer':
            user_list=[]
            for i in range(10):
                Your_pass = random.choice(string.ascii_letters + string.digits)
                user_list.append(Your_pass)
            Pass_result = "".join(user_list)
            print(user.name,Pass_result)
            conn=engine.connect()
            conn.execute("insert into ods_tableau_user VALUES (NULL,'%s','%s','%s',SYSDATE)" %(user.name,'Viewer',Pass_result))
            conn.close()
            server.users.update(user, Pass_result)

    # all_sites,pagination_item = server.sites.get()
    # for site in all_sites:
    #     print(site.id, site.name, site.content_url, site.state)
    # s_info = server.server_info.get()
    # print("\nServer info:")
    # print("\tProduct version: {0}".format(s_info.product_version))
    # print("\tREST API version: {0}".format(s_info.rest_api_version))
    # print("\tBuild number: {0}".format(s_info.build_number))

    # request_options = TSC.RequestOptions(pagesize=1000)
    # all_workbooks = list(TSC.Pager(server.workbooks, request_options))
    # print([wb.name for wb in TSC.Pager(server.workbooks)])