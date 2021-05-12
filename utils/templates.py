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