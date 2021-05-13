from string import Template

"""Insert Query template"""
db_insert = Template("INSERT INTO ${voms_tbl} (subject, issuer, vo_id)"
                     " SELECT curr.subject, curr.issuer, curr.vo_id"
                     " FROM ${voms_tbl}_temp curr LEFT JOIN ${voms_tbl} prev"
                     " ON curr.subject=prev.subject" 
                     " AND curr.issuer=prev.issuer" 
                     " AND curr.vo_id=prev.vo_id WHERE prev.subject IS NULL")


defaults_db_insert = {
    "voms_tbl": "voms_members"
}

"""Delete Query template"""
db_delete = Template("DELETE FROM ${voms_tbl} t1 USING ("
                     " SELECT prev.subject, prev.issuer, prev.vo_id"
                     " FROM ${voms_tbl} prev LEFT JOIN ${voms_tbl}_temp curr"
                     " ON curr.subject=prev.subject" 
                     " AND curr.issuer=prev.issuer" 
                     " AND curr.vo_id=prev.vo_id WHERE curr.subject IS NULL) sq"
                     " WHERE sq.subject=t1.subject"
                     " AND sq.issuer=t1.issuer"
                     " AND sq.vo_id=t1.vo_id")


defaults_db_delete = {
    "voms_tbl": "voms_members",
}

""" Create Temp Table """
tbl_tmp_create = Template("CREATE TEMP TABLE ${voms_tbl}_temp ("
                          " id integer PRIMARY KEY,"
                          " subject character varying(256) NOT NULL,"
                          " issuer character varying(256) NOT NULL,"
                          " vo_id character varying(256) NOT NULL,"
                          " created timestamp without time zone)")

defaults_tbl_tmp_create = {
    "voms_tbl": "voms_members",
}

""" Insert in Temp Table """
tbl_tmp_insert = Template("INSERT INTO ${voms_tbl}_temp (id, subject, issuer, vo_id, created) VALUES %s")

defaults_tbl_tmp_insert = {
    "voms_tbl": "voms_members",
}

""" Delete from Temp Table """
tbl_tmp_delete = Template("DELETE FROM ${voms_tbl}_temp"
                          " WHERE id IN (SELECT id FROM ("
                          " SELECT id, ROW_NUMBER() OVER ("
                          " partition BY subject, issuer, vo_id ORDER BY id) AS rnum"
                          " FROM ${voms_tbl}_temp) t WHERE t.rnum > 1)")

defaults_tbl_tmp_delete = {
    "voms_tbl": "voms_members",
}