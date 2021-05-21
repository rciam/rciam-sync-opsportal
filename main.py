#
#  Copyright 2017-2020 GRNET S.A.
# 
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import config

import json
import psycopg2
import psycopg2.extras
import requests
import utils.templates as tpl

from datetime import datetime

__author__    = "Nicolas Liampotis"
__email__     = "nliam@grnet.gr"
__version__   = "$Revision: 1.0.0"
__date__      = "$Date: 2020-09-09 08:50:22"
__copyright__ = "Copyright (c) 2017-2020 GRNET S.A."
__license__   = "Apache Licence v2.0"

vo_members_url = config.opsportal['api']['base_url'] + \
    "/api/vo/users/json"

api_key = config.opsportal['api']['key']

dsn = "dbname=" + config.registry['db']['name'] + \
    " user=" + config.registry['db']['user'] + \
    " password=" + config.registry['db']['password'] + \
    " host=" + config.registry['db']['host'] + \
    " sslmode=require"

vo_members_tbl = config.registry['db']['voms_tbl']


def main():
    now = datetime.utcnow()
    remote_members = get_remote_members()
    update_local_members(remote_members, now)


def get_remote_members():
    members = []
    header = {"X-API-Key": api_key}
    try:
        r = requests.get(vo_members_url, headers=header)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)
    data = r.json()

    for index in range (len(data["result"])):
        subject = None
        issuer = None
        vo_id = None
        for column in data["result"][index]["row"]:
            if 'CERTDN' in column: 
                subject = column["CERTDN"]
            elif 'CA' in column:
                issuer = column["CA"]
            elif 'VO' in column:
                vo_id = column["VO"]
        if not subject or not issuer or not vo_id:
            continue
        members.append({'subject': subject[0].strip(), 'issuer': issuer[0].strip(), 'vo_id': vo_id[0].strip()})

    return members


def update_local_members(members, timestamp):
    values_list = []
    row_id = 1
    for member in members:
        values = (row_id, member['subject'], member['issuer'], member['vo_id'],
                  timestamp)
        row_id += 1
        values_list.append(values)

    conn = psycopg2.connect(dsn)
    with conn:
        create_tmp_sql = tpl.tbl_tmp_create.substitute(tpl.defaults_tbl_tmp_create, voms_tbl=vo_members_tbl)
        with conn.cursor() as curs:
            curs.execute(create_tmp_sql)

        insert_tmp_sql = tpl.tbl_tmp_insert.substitute(tpl.defaults_tbl_tmp_insert, voms_tbl=vo_members_tbl)
        with conn.cursor() as curs:
            psycopg2.extras.execute_values(curs, insert_tmp_sql, values_list,
                                           page_size=1000)

        # Remove duplicate remote membership info
        delete_tmp_sql = tpl.tbl_tmp_delete.substitute(tpl.defaults_tbl_tmp_delete, voms_tbl=vo_members_tbl)
        with conn.cursor() as curs:
            curs.execute(delete_tmp_sql)

        # Add new members
        insert_sql = tpl.db_insert.substitute(tpl.defaults_db_insert, voms_tbl=vo_members_tbl)
        with conn.cursor() as curs:
            curs.execute(insert_sql)

        # Remove stale members
        del_sql = tpl.db_delete.substitute(tpl.defaults_db_delete, voms_tbl=vo_members_tbl)
        with conn.cursor() as curs:
            curs.execute(del_sql)

    conn.close()


if __name__ == "__main__":
    main()

