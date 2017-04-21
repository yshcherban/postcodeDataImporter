/**
 * Import students from CSV to Database
 */

const   fs          = require('fs'),
		Promise     = require('bluebird'),
		MongoClient = require('mongodb').MongoClient,
		ObjectId    = require('mongodb').ObjectId,
		baby        = require('babyparse'),
		path        = require('path'),
		mime        = require('mime'),
		propz		= require('propz');

const   dbConnect           = Promise.promisify(MongoClient.connect),
		fileExist           = Promise.promisify(fs.stat);

const   hostname                		= 'mongodb://127.0.0.1:27017',
		dbName                  		= 'squad-server2-1',
		inputPostcodeCollectionName		= 'postcodes',
		outputPostcodeCollectionName	= 'postcodes2';


const   fileCsvName             = 'doogal.csv'; //'National_Statistics_Postcode_Lookup_UK.csv'; /* Postcode, CountyName */

let dbReference;

/**
 * Searching for provided postcodeNoSpaces in postcodeArray and returning county name if its found
 * @param {Array.<*>} postcodeArray
 * @param {String} postcodeNoSpaces
 * @returns {String|undefined} county found if any
 */
function findPostcodeCounty(postcodeArray, postcodeNoSpaces) {
	postcodeNoSpaces = postcodeNoSpaces.toUpperCase();
	const foundPostcodeDetails = '';//postcodeArray.find( postcodeItem => {
	// 	const upperCasedPostcodeFromArray = postcodeItem[0].replace(/\s/g,'').toUpperCase();
	// 	return postcodeNoSpaces === upperCasedPostcodeFromArray;
	// });


	const result = propz.get(foundPostcodeDetails, [1]);
	console.log('searching for postcode: ' + postcodeNoSpaces + ' found: ' + result);
	return result;
}

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
		const parsedResponse = baby.parseFiles(file, {
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
	await fileExist(fileCsvName);
	if(canParseFile(fileCsvName)) {
		const db = await dbConnect(`${hostname}/${dbName}`);

		const	inputPostcodeCollection		= db.collection(inputPostcodeCollectionName),
				outputPostcodeCollection	= db.collection(outputPostcodeCollectionName),
				inputPostcodeCursor			= inputPostcodeCollection.find();

		const postcodeArrFromFile = await parse(fileCsvName);
		console.log('Starting..');
		let promises = [];

		inputPostcodeCursor.forEach(postcodeObj => {
			const optCounty = findPostcodeCounty(postcodeArrFromFile, postcodeObj.postcodeNoSpaces);
			console.log(postcodeObj.postcodeNoSpaces);
			if(optCounty) {
				const postcodeDataToInsert = Object.assign({}, postcodeObj, { county: optCounty });
				console.log('data to insert: ' + JSON.stringify(postcodeDataToInsert, null, 2));
				const insertPromise = outputPostcodeCollection.insertOne(postcodeDataToInsert);
				insertPromise.then(() => console.log('inserted!'), err => console.log('not inserted: ' + err));
				promises.push(insertPromise);
			}
		}, async () => {
			await Promise.all(promises);
			db.close();
			console.log('Successfully completed.');
		});

	} else {
		console.err('Cannot parse file: ' + fileCsvName);
	}
}


main();


