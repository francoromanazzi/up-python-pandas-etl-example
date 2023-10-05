create schema up_dw;

create table up_dw.lk_empresa (
	sk_empresa INT primary key,
	descripcion VARCHAR(255)
);

create table up_dw.lk_fecha (
	sk_fecha VARCHAR(8) primary key
);

create table up_dw.lk_medio_pago (
	sk_medio_pago INT primary key,
	descripcion VARCHAR(255)
);


create table up_dw.bt_pago (
	sk_pago INT,
	sk_item INT,
	sk_empresa INT references up_dw.lk_empresa,
	sk_fecha VARCHAR(8) references up_dw.lk_fecha,
	sk_medio_pago INT references up_dw.lk_medio_pago,
	importe NUMERIC(15, 2),
	primary key (sk_pago, sk_item, sk_empresa, sk_fecha, sk_medio_pago)
);