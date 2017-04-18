/**
 * Import students from CSV to Database
 */

const   fs          = require('fs'),
		Promise     = require('bluebird'),
		MongoClient = require('mongodb').MongoClient,
		ObjectId    = require('mongodb').ObjectId,
		baby        = require('babyparse'),
		path        = require('path'),
		mime        = require('mime');

const   dbConnect           = Promise.promisify(MongoClient.connect),
		fileExist           = Promise.promisify(fs.stat);

const   hostname                = 'mongodb://127.0.0.1:27017',
		dbName                  = 'squad-server2-1',
		postcodeCollectionName  = 'postcodes';

const   fileCsvName             = 'National_Statistics_Postcode_Lookup_UK.csv'; /* Postcode, CountyName */

let dbReference;

fileExist(fileCsvName).then( res => {

	if (canParseFile(fileCsvName)) {

		dbConnect(`${hostname}/${dbName}`).then(db => {
			dbReference = db;
			const postcodeCollection = db.collection(postcodeCollectionName);
			const postcodeCursor = postcodeCollection.find();
			
			return parse(fileCsvName).then(postcodeArrFromFile => {
						
				const postcodeArrPromises = postcodeArrFromFile.map(postcodeData => {
					const postcode = postcodeData[0].replace(/\s/g,''); /** remove spaces from string */
					const countyName = postcodeData[1];

					if (postcode && countyName)

						return postcodeCollection.update(
							{ "postcodeNoSpaces": postcode },
							{
								$set: {
									"county": countyName
								}
							}
						);

				});

				return Promise.all(postcodeArrPromises).then(res => {
					dbReference.close();
					console.log('Successfully completed.');
				});

			}).catch(e => {
				console.log('Check file contents. File can not be empty!');
			});

		}).catch(e => {
			console.log('Can\'t connect to database');
		})

	} else {
		console.log("Wrong file type");
	}

}).catch(e => {
	console.log('File not found');
});


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