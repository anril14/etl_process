insert into ods.vendor(id, description)
	values (1, 'Creative Mobile Technologies, LLC'),
		(2, 'Curb Mobility, LLC'),
		(6, 'Myle Technologies Inc'),
		(7, 'Helix');

insert into ods.ratecode(id, description)
	values (1, 'Standard rate'),
		(2, 'JFK'),
		(3, 'Newark'),
		(4, 'Nassau or Westchester'),
		(5, 'Negotiated fare'),
		(6, 'Group ride'),
		(99, 'Null/unknown');

insert into ods.payment(id, description)
	values (0, 'Flex Fare trip'),
		(1, 'Credit card'),
		(2, 'Cash'),
		(3, 'No charge'),
		(4, 'Dispute'),
		(5, 'Unknown'),
		(6, 'Voided trip');