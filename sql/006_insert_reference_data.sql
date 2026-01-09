INSERT INTO ods.vendor(id, description)
	VALUES (1, 'Creative Mobile Technologies, LLC'),
		(2, 'Curb Mobility, LLC'),
		(6, 'Myle Technologies Inc'),
		(7, 'Helix');

INSERT INTO ods.ratecode(id, description)
	VALUES (1, 'Standard rate'),
		(2, 'JFK'),
		(3, 'Newark'),
		(4, 'Nassau or Westchester'),
		(5, 'Negotiated fare'),
		(6, 'Group ride'),
		(99, 'Null/unknown');

INSERT INTO ods.payment(id, description)
	VALUES (0, 'Flex Fare trip'),
		(1, 'Credit card'),
		(2, 'Cash'),
		(3, 'No charge'),
		(4, 'Dispute'),
		(5, 'Unknown'),
		(6, 'Voided trip');