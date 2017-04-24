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
		collectionName  		= 'postcodes';

const 	fileExist           	= Promise.promisify(fs.stat),
		fileCsvName             = 'postcodes-doog.csv'; // postcodes-doog.csv //'National_Statistics_Postcode_Lookup_UK.csv'; /* Postcode, CountyName */


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


async function saveCounty(postcodeCollection, postcodeIdFromDb, postcodeFromDb, parsedDataFromFile) {

	for (let data of parsedDataFromFile) {
		const postcodeFromFile = data[0].replace(/\s/g,'').toUpperCase();
		const countyName = data[1];

		if ((postcodeFromDb === postcodeFromFile) && countyName) {
			console.log('Updated');
			await postcodeCollection.update(
					{ "_id": postcodeIdFromDb },
					{ $set: { "county": countyName } }
				  );
		}
	}

}


async function main() {
	try {
		/** checks file */
		await fileExist(fileCsvName);
		if (!canParseFile(fileCsvName)) throw Error('Can\'t parse file');
		/** checks db */
		const db = await dbConnect(`${host}/${dbName}`);
		const postcodeCollection = db.collection(collectionName);
		const docCollectionArr = db.collection(collectionName).find().addCursorFlag('noCursorTimeout', true);
		const parsedDataFromFile = await parse(fileCsvName);
		

		for (let doc = await docCollectionArr.next(); doc != null; doc = await docCollectionArr.next()) {
			const postcodeIdFromDb = doc._id;
			const postcodeFromDb = doc.postcodeNoSpaces.toUpperCase();
			
			console.log('start');
			//await saveCounty(parsedDataFromFile);
			await saveCounty(postcodeCollection, postcodeIdFromDb, postcodeFromDb, parsedDataFromFile);
			console.log('end');
			


  		}

  		db.close();
		console.log('Successfully completed.');


	} catch (e) {
		console.log(e.message);
	}
	
}

main();