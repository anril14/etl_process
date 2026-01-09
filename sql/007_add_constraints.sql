ALTER TABLE ods.taxi_data
ADD CONSTRAINT fk__taxi__data_vendor_id__vendor__id FOREIGN KEY (vendor_id) REFERENCES ods.vendor(id)
		ON DELETE NO ACTION,
ADD CONSTRAINT fk__taxi__ratecode_id__ratecode__id FOREIGN KEY (ratecode_id) REFERENCES ods.ratecode(id)
		ON DELETE NO ACTION,
ADD CONSTRAINT fk__taxi__payment_type__payment__id FOREIGN KEY (payment_type) REFERENCES ods.payment(id)
		ON DELETE NO ACTION;