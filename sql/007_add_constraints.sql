alter table ods.taxi_data
add constraint fk__taxi__data_vendor_id__vendor__id foreign key (vendor_id) references ods.vendor(id)
		on delete no action,
add constraint fk__taxi__ratecode_id__ratecode__id foreign key (ratecode_id) references ods.ratecode(id)
		on delete no action,
add constraint fk__taxi__payment_type__payment__id foreign key (payment_type) references ods.payment(id)
		on delete no action;