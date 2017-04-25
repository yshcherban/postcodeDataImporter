/**
 * Import students from CSV to Database
 */

const   fs          			= require('fs'),
		Promise     			= require('bluebird'),
		MongoClient 			= require('mongodb').MongoClient,
		ObjectId    			= require('mongodb').ObjectId,
		babyParse        		= require('babyparse'),
		path        			= require('path'),
		mime        			= require('mime');

const   dbConnect           	= Promise.promisify(MongoClient.connect),
		host                	= 'mongodb://127.0.0.1:27017',
		dbName                  = 'squad-server2-1',
		inputCollectionName  	= 'postcodes',
		outputCollectioName		= 'postcodes2';

const 	fileExist           	= Promise.promisify(fs.stat),
		fileCsvName             = 'National_Statistics_Postcode_Lookup_UK.csv'; // postcodes-doog.csv //'National_Statistics_Postcode_Lookup_UK.csv'; /* Postcode, CountyName */


function parse (file) {
	return getPromiseFromCSVFile(file).then(result => {
		return result.data || [];
	});
}

function canParseFile (file) {
	return (path.extname(file) === '.csv') || (mime.lookup(file) === 'text/csv');
}

function getPromiseFromCSVFile (file) {
	return new Promise((resolve, reject) => {
		const parsedResponse = babyParse.parseFiles(file, {
			header:     false,
			complete:   (results, file) => {
				/** throw Error for reading invalid file */
				if (results.errors.length > 0) {
					reject(new Error(results.errors[0].message));
				} else {
					resolve(results)
				}
			}
		});
		/** throw Error if file is unsupported */
		if (parsedResponse.errors.length > 0) {
			reject(new Error(parsedResponse.errors[0].message));
		}
		// no return
	});
}

async function main() {
	try {
		/** checks file */
		await fileExist(fileCsvName);
		if (!canParseFile(fileCsvName)) throw Error('Can\'t parse file');
		/** checks db */
		const db = await dbConnect(`${host}/${dbName}`);
		
		const originalPostcodeCollection = db.collection(inputCollectionName);

		console.log('cloning postcode collection');
		const sourcePostcodeCollection = await originalPostcodeCollection.find().toArray();
		const targetPostcodeCollection = await db.createCollection(outputCollectioName);
		await targetPostcodeCollection.insert(sourcePostcodeCollection);
		
		console.log('Parsing file');
		const parsedDataFromFile = await parse(fileCsvName);
		console.log('Loading data from file in database');
		
		for (let data of parsedDataFromFile) {
			const postcodeFromFile = data[0].replace(/\s/g,'').toUpperCase();
			const countyName = data[1];

			if (postcodeFromFile && countyName) {

			await postcodeCollection.update(
					{ "postcodeNoSpaces": postcodeFromFile.replace(/\s/g,'').toUpperCase() },
					{ $set: { "county": countyName } }
				  );

			}
		}

  		db.close();
		console.log('Successfully completed.');
		
	} catch (e) {
		console.log(e.message);
	}
	
}

main();