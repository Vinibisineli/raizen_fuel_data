with my_table as(
SELECT combustivel, ano, ul.uf as c_uf, regiao, rd.estado, jan, fev, mar, abr, mai, jun, jul, ago, set, out, nov, dez, total
FROM raizen_diesel rd
left join uf_lookup ul on ul.estado=rd.estado ),
jan as (select to_date(concat(ano,'01'),'YYYYMM') as ano_mes,c_uf,combustivel,jan as mes from my_table),
fev as (select to_date(concat(ano,'02'),'YYYYMM') as ano_mes,c_uf,combustivel,fev as mes from my_table),
mar as (select to_date(concat(ano,'03'),'YYYYMM') as ano_mes,c_uf,combustivel,mar as mes from my_table),
abr as (select to_date(concat(ano,'04'),'YYYYMM') as ano_mes,c_uf,combustivel,abr as mes from my_table),
mai as (select to_date(concat(ano,'05'),'YYYYMM') as ano_mes,c_uf,combustivel,mai as mes from my_table),
jun as (select to_date(concat(ano,'06'),'YYYYMM') as ano_mes,c_uf,combustivel,jun as mes from my_table),
jul as (select to_date(concat(ano,'07'),'YYYYMM') as ano_mes,c_uf,combustivel,jul as mes from my_table),
ago as (select to_date(concat(ano,'08'),'YYYYMM') as ano_mes,c_uf,combustivel,ago as mes from my_table),
set as (select to_date(concat(ano,'09'),'YYYYMM') as ano_mes,c_uf,combustivel,set as mes from my_table),
out as (select to_date(concat(ano,'10'),'YYYYMM') as ano_mes,c_uf,combustivel,out as mes from my_table),
nov as (select to_date(concat(ano,'11'),'YYYYMM') as ano_mes,c_uf,combustivel,nov as mes from my_table),
dez as (select to_date(concat(ano,'12'),'YYYYMM') as ano_mes,c_uf,combustivel,dez as mes from my_table),
union_table as (
select * from jan
union all
select * from fev
union all
select * from mar
union all
select * from abr
union all
select * from mai
union all
select * from jun
union all
select * from jul
union all
select * from ago
union all
select * from set
union all
select * from out
union all
select * from nov
union all
select * from dez
)
insert into public.sales_diesel (year_month,uf,product,volume)
select ano_mes as year_month,
c_uf as uf,
combustivel as product,
mes as volume
from union_table
ON CONFLICT DO NOTHING;