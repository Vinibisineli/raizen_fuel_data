with my_table as(
SELECT "COMBUSTÍVEL", "ANO", ul.uf as c_uf, "REGIÃO", "ESTADO", "Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez", "TOTAL"
FROM public.raizen_disel rd
left join public.uf_lookup ul on ul.estado=rd."ESTADO" ),
jan as (select to_date(concat("ANO",'01'),'YYYYMM') as ano_mes,c_uf,"COMBUSTÍVEL","Jan" as mes from my_table),
fev as (select to_date(concat("ANO",'02'),'YYYYMM') as ano_mes,c_uf,"COMBUSTÍVEL","Fev" as mes from my_table),
mar as (select to_date(concat("ANO",'03'),'YYYYMM') as ano_mes,c_uf,"COMBUSTÍVEL","Mar" as mes from my_table),
abr as (select to_date(concat("ANO",'04'),'YYYYMM') as ano_mes,c_uf,"COMBUSTÍVEL","Abr" as mes from my_table),
mai as (select to_date(concat("ANO",'05'),'YYYYMM') as ano_mes,c_uf,"COMBUSTÍVEL","Mai" as mes from my_table),
jun as (select to_date(concat("ANO",'06'),'YYYYMM') as ano_mes,c_uf,"COMBUSTÍVEL","Jun" as mes from my_table),
jul as (select to_date(concat("ANO",'07'),'YYYYMM') as ano_mes,c_uf,"COMBUSTÍVEL","Jul" as mes from my_table),
ago as (select to_date(concat("ANO",'08'),'YYYYMM') as ano_mes,c_uf,"COMBUSTÍVEL","Ago" as mes from my_table),
set as (select to_date(concat("ANO",'09'),'YYYYMM') as ano_mes,c_uf,"COMBUSTÍVEL","Set" as mes from my_table),
out as (select to_date(concat("ANO",'10'),'YYYYMM') as ano_mes,c_uf,"COMBUSTÍVEL","Out" as mes from my_table),
nov as (select to_date(concat("ANO",'11'),'YYYYMM') as ano_mes,c_uf,"COMBUSTÍVEL","Nov" as mes from my_table),
dez as (select to_date(concat("ANO",'12'),'YYYYMM') as ano_mes,c_uf,"COMBUSTÍVEL","Dez" as mes from my_table),
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
"COMBUSTÍVEL" as product,
mes as volume
from union_table
ON CONFLICT DO NOTHING;