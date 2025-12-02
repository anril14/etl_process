create schema if not exists stg;
create schema if not exists ods;
create schema if not exists dm;

comment on schema stg is 'Staging';
comment on schema ods is 'Operational data store';
comment on schema dm is 'Data marts';
