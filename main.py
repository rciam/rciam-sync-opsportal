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
    row_id = 1;
    for member in members:
        values = (row_id, member['subject'], member['issuer'], member['vo_id'],
                  timestamp)
        row_id += 1;
        values_list.append(values)

    conn = psycopg2.connect(dsn)
    with conn:
        sql = """CREATE TEMP TABLE voms_members_temp (
                 id integer PRIMARY KEY,
                 subject character varying(256) NOT NULL,
                 issuer character varying(256) NOT NULL,
                 vo_id character varying(256) NOT NULL,
                 created timestamp without time zone)"""
        with conn.cursor() as curs:
            curs.execute(sql)

        sql = """INSERT INTO voms_members_temp (id, subject, issuer, vo_id,
                 created) VALUES %s"""
        with conn.cursor() as curs:
            psycopg2.extras.execute_values(curs, sql, values_list,
                                           page_size=1000)

        # Remove duplicate remote membership info
        sql = """DELETE FROM voms_members_temp
                 WHERE id IN (SELECT id FROM (
                 SELECT id, ROW_NUMBER() OVER (
                 partition BY subject, issuer, vo_id ORDER BY id) AS rnum 
                 FROM voms_members_temp) t WHERE t.rnum > 1)"""
        with conn.cursor() as curs:
            curs.execute(sql)

        # Add new members
        sql = """INSERT INTO voms_members (subject, issuer, vo_id) 
                 SELECT curr.subject, curr.issuer, curr.vo_id
                 FROM voms_members_temp curr LEFT JOIN voms_members prev 
                 ON curr.subject=prev.subject 
                 AND curr.issuer=prev.issuer 
                 AND curr.vo_id=prev.vo_id WHERE prev.subject IS NULL"""
        with conn.cursor() as curs:
            curs.execute(sql)

        # Remove stale members
        sql = """DELETE FROM voms_members t1 USING (
                 SELECT prev.subject, prev.issuer, prev.vo_id
                 FROM voms_members prev LEFT JOIN voms_members_temp curr 
                 ON curr.subject=prev.subject 
                 AND curr.issuer=prev.issuer 
                 AND curr.vo_id=prev.vo_id WHERE curr.subject IS NULL) sq
                 WHERE sq.subject=t1.subject
                 AND sq.issuer=t1.issuer
                 AND sq.vo_id=t1.vo_id"""
        with conn.cursor() as curs:
            curs.execute(sql)

    conn.close()


if __name__ == "__main__":
    main()

