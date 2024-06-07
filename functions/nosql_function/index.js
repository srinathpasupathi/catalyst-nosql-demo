'use strict';
var express = require('express');
var catalyst = require('zcatalyst-sdk-node');
const { NoSQLItem, NoSQLMarshall } = require('zcatalyst-sdk-node/lib/no-sql');
var app = express();
app.use(express.json());

app.post('/insertWithoutConditions', async (req, res) => {
	var catalystApp = catalyst.initialize(req);
	const nosql = catalystApp.nosql();
	const ordersInstance = nosql.table('Orders');

	try {
		const item = new NoSQLItem()
			.addString('OrderId', 'O1001')
			.addString('OrderDate', '2023-06-01')
			.addString('CustomerId', 'C123')
			.addString('Status', 'Pending')
			.addNumber('TotalAmount', 2040.00)
			.addMap('Items', {
				item1: {
					name: 'Laptop',
					price: 2000.00,
					quantity: 2
				},
				item2: {
					name: 'Mouse',
					price: 20.00,
					quantity: 2
				}
			});

		const plainInsert = await ordersInstance.insertItems({
			item: item
		});

		res.status(200).json({
			message: 'Item inserted successfully',
			data: plainInsert
		});
	} catch (error) {
		console.error('Error inserting item:', error);
		res.status(500).json({
			message: 'Error inserting item',
			error: error
		});
	}
});

app.post('/insertWithConditions', async (req, res) => {
	var catalystApp = catalyst.initialize(req);
	const nosql = catalystApp.nosql();
	const ordersInstance = nosql.table('Orders');
	try {
		const item = new NoSQLItem()
			.addString('OrderId', 'O1002')
			.addString('OrderDate', '2023-06-01')
			.addString('CustomerId', 'C123')
			.addString('Status', 'Shipped')
			.addNumber('TotalAmount', 1040.00)
			.addMap('Items', {
				item1: {
					name: 'Laptop',
					price: 1000.00,
					quantity: 1
				},
				item2: {
					name: 'Mouse',
					price: 20.00,
					quantity: 2
				}
			});

		const conditionalInsert = await ordersInstance.insertItems({
			item: item,
			condition: {
				attribute: 'OrderId',
				operator: "not_equals",
				value: NoSQLMarshall.makeString('O1002')
			}
		});

		res.status(200).json({
			data: conditionalInsert
		});
	} catch (error) {
		console.error('Error inserting item:', error);
		res.status(500).json({
			message: 'Error inserting item',
			error: error
		});
	}
});

app.put('/updateItem', async (req, res) => {
	var catalystApp = catalyst.initialize(req);
	const nosql = catalystApp.nosql();
	const ordersInstance = nosql.table('Orders');

	try {
		const updatedItems = await ordersInstance.updateItems({
			keys: new NoSQLItem()
				.addString('OrderId', 'O1001')
				.addString('OrderDate', '2023-06-01'),
			update_attributes: [
				{
					operation_type: "PUT",
					update_value: NoSQLMarshall.makeString('Shipped'),
					attribute_path: ['Status']
				}
			]
		});

		res.status(200).json({
			message: 'Item updated successfully',
			data: updatedItems
		});
	} catch (error) {
		console.error('Error updating item:', error);
		res.status(500).json({
			message: 'Error updating item',
			error: error.message
		});
	}
});


app.get('/fetchItem', async (req, res) => {
	var catalystApp = catalyst.initialize(req);
	const nosql = catalystApp.nosql();
	const ordersInstance = nosql.table('Orders');

	try {
		const fetchedItem = await ordersInstance.fetchItem({
			keys: [new NoSQLItem().addString('OrderId', 'O1001')
				.addString('OrderDate', '2023-06-01')
			],
			consistent_read: true,
			required_attributes: [['Status'], ['Items'], ['CustomerId'], ['TotalAmount']]
		});

		res.status(200).json({
			message: 'Item fetched successfully',
			data: fetchedItem
		});

	} catch (error) {
		console.error('Error fetching item:', error);
		res.status(500).json({
			message: 'Error fetching item',
			error: error
		});
	}
});

app.get('/queryItems', async (req, res) => {
	var catalystApp = catalyst.initialize(req);
	const nosql = catalystApp.nosql();
	const ordersInstance = nosql.table('Orders');

	try {
		const queriedItem = await ordersInstance.queryTable({
			key_condition: {
				attribute: 'OrderId',
				operator: "equals",
				value: NoSQLMarshall.makeString('O1001')
			},
			consistent_read: true,
			limit: 10,
			forward_scan: true
		});

		res.status(200).json({
			message: 'Query executed successfully',
			data: queriedItem
		});

	} catch (error) {
		console.error('Error querying item:', error);
		res.status(500).json({
			message: 'Error querying item',
			error: error
		});
	}
});

app.get('/queryIndex', async (req, res) => {
	var catalystApp = catalyst.initialize(req);
	const nosql = catalystApp.nosql();
	const ordersInstance = nosql.table('Orders');

	try {
		const queriedIndexItem = await ordersInstance.queryIndex('2628000002824058', {
			"key_condition": {
				"group_operator": "and",
				"group": [
					{
						"attribute": "CustomerId",
						"operator": "equals",
						"value": { "S": "C123" }
					}, {
						"attribute": "OrderDate",
						"operator": "begins_with",
						"value": { "S": "2023" }
					}
				]},
				consistent_read: true,
				limit: 15,
				forward_scan: true
			});

		res.status(200).json({
			message: 'Index Query executed successfully',
			data: queriedIndexItem,
		});

	} catch (error) {
		console.error('Error querying the index for items', error);
		res.status(500).json({
			message: 'Error querying the index for items',
			error: error
		});
	}
});

app.delete('/deleteItem', async (req, res) => {
	var catalystApp = catalyst.initialize(req);
	const nosql = catalystApp.nosql();
	const ordersInstance = nosql.table('Orders');

	try {
		const deletedItems = await ordersInstance.deleteItems({
			keys: NoSQLItem.from({ OrderId: 'O1001', OrderDate: '2023-06-01' })
		});

		res.status(200).json({
			message: 'Item deleted successfully',
			data: deletedItems
		});

	} catch (error) {
		console.error('Error while deleting item', error);
		res.status(500).json({
			message: 'Error while deleting items',
			error: error
		});
	}
});

function sendErrorResponse(res) {
	res.status(500);
	res.send({
		"error": "Internal server error occurred. Please try again in some time."
	});
}
module.exports = app;