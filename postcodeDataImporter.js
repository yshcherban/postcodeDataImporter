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

async function loadCollectionToArray(collection, displayProgress = true) {
	const	cursor	= collection.find(),
			total	= await collection.count();

	let	items		= [],
		itemsCount	= 0;

	return new Promise( (resolve, reject) => {
		cursor.forEach( item => {
			items.push(item);
			itemsCount++;
			if(displayProgress && itemsCount % 10000 === 0) {
				console.log('processed: ' + itemsCount / total * 100 + '%');
			}
		}, () => {
			resolve(items);
		});
	});
}

class PostcodeDirectory {
	constructor(postcodesArr) {
		const postcodeDirectoryArray = postcodesArr.map( postcodeItem => {
			postcodeItem[0] = postcodeItem[0].replace(/\s/g,'').toUpperCase();
			return postcodeItem;
		});
		this.__postcodeDirectoryArray = postcodeDirectoryArray;
	}

	find(postcodeNoSpaces) {
		return this.__postcodeDirectoryArray.find( p => p[0] === postcodeNoSpaces );
	}

	size() {
		return this.__postcodeDirectoryArray.length;
	}
}

class PostcodeDirectory2 {
	constructor(postcodesArr) {
		const directory = {};
		postcodesArr.forEach( postcodeItem => {
			postcodeItem[0] = postcodeItem[0].replace(/\s/g,'').toUpperCase();
			const firstLetter = postcodeItem[0].charAt(0);
			directory[firstLetter] = directory[firstLetter] || [];
			directory[firstLetter].push(postcodeItem);
		});
		this.__directory = directory;
		this.__length = postcodesArr.length;
		console.log('directory built');
	}

	find(postcodeNoSpaces) {
		const firstLetter = postcodeNoSpaces.charAt(0);
		const subDirectory = this.__directory[firstLetter] || [];
		return subDirectory.find( p => p[0] === postcodeNoSpaces );
	}

	size() {
		return this.__length;
	}
}

async function main() {
	await fileExist(fileCsvName);
	if(canParseFile(fileCsvName)) {
		const db = await dbConnect(`${hostname}/${dbName}`);

		const	inputPostcodeCollection		= db.collection(inputPostcodeCollectionName),
				outputPostcodeCollection	= db.collection(outputPostcodeCollectionName);

		const postcodeArrFromFile = await parse(fileCsvName);
		console.log('Loaded original postcodes');

		const postcodeDirectory = new PostcodeDirectory2(postcodeArrFromFile);

		console.log(`Postcode directory built. There are ${postcodeDirectory.size()} items`);

		const dbPostcodesArray = await loadCollectionToArray(inputPostcodeCollection);
		console.log('Mongo collection dumped in memory: ' + dbPostcodesArray.length);

		let processedPostcodesCount = 0,
			totalPostcodesCount		= dbPostcodesArray.length;

		console.log('Processing postcodes..');
		dbPostcodesArray.forEach( dbPostcode => {
			const postcodeNoSpaces			= dbPostcode.postcodeNoSpaces;
			const foundDirectoryPostcode	= postcodeDirectory.find(postcodeNoSpaces);
			if(foundDirectoryPostcode && foundDirectoryPostcode[1]) {
				dbPostcode.county = foundDirectoryPostcode[1];
			}
			processedPostcodesCount++;
			if(processedPostcodesCount % 100 === 0 ) {
				console.log('processed: ' + processedPostcodesCount / totalPostcodesCount * 100 + '% ' + processedPostcodesCount );
			}
		});

		console.log('Done!');

	} else {
		console.err('Cannot parse file: ' + fileCsvName);
	}
}


main();




