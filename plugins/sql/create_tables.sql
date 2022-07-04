CREATE TABLE IF NOT EXISTS sales_oil(
year_month date,
uf varchar(50),
product varchar(200),
unit varchar(200),
volume double precision,
created_at timestamp DEFAULT now(),
unique (year_month,uf,product)
);

CREATE TABLE IF NOT EXISTS sales_diesel(
year_month date,
uf varchar(50),
product varchar(200),
unit varchar(200),
volume double precision,
created_at timestamp DEFAULT now(),
unique (year_month,uf,product)
);


CREATE TABLE IF NOT EXISTS uf_lookup(
uf char(2) not null,
estado varchar(100) not null,
unique (uf)
);
CREATE INDEX IF NOT EXISTS idx_estado ON uf_lookup (estado);

INSERT INTO uf_lookup (uf,estado) VALUES
('AC','ACRE'),
('AL','ALAGOAS'),
('AM','AMAZONAS'),
('AP','AMAPÁ'),
('BA','BAHIA'),
('CE','CEARÁ'),
('DF','DISTRITO FEDERAL'),
('ES','ESPÍRITO SANTO'),
('GO','GOIÁS'),
('MA','MARANHÃO'),
('MG','MINAS GERAIS'),
('MS','MATO GROSSO DO SUL'),
('MT','MATO GROSSO'),
('PA','PARÁ'),
('PB','PARAÍBA'),
('PE','PERNAMBUCO'),
('PI','PIAUÍ'),
('PR','PARANÁ'),
('RJ','RIO DE JANEIRO'),
('RN','RIO GRANDE DO NORTE'),
('RO','RONDÔNIA'),
('RR','RORAIMA'),
('RS','RIO GRANDE DO SUL'),
('SC','SANTA CATARINA'),
('SE','SERGIPE'),
('SP','SÃO PAULO'),
('TO','TOCANTINS')
ON CONFLICT DO NOTHING;