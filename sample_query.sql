select count(0),name,to_char(created,'mm/dd')
from trx.trx t, trx.users u
where t.userid=u.userid
and created > sysdate-3
group by name,to_char(created,'mm/dd')
/
