// on ouvre un client redis
var redis = require("redis");
var fs = require("fs");

// save csv file tohash element
// 	var keyname='article:123:*';
// 	var filename=__dirname+'/data.csv';
//	redisCSV.csvToHash(filename,keyname,'codebarre',cb);
function csvToHash(filename,keyTableName,fieldKey,cb,separator){
	var separator=(typeof separator === "undefined") ?';':separator;
	var client = redis.createClient();
	
	fs.readFile(filename, function(err, data) {
		//if error return error
		if (err) cb (err)
		//only for bench
		var startDate =new Date();
		// split the file in line
		var lines = data.toString().split("\n");
		//get the first line ie headers
		var headers=lines[0].replace(/[\n\r]/g,"").split(";");
		// get fieldKey position
		var posKey=getFieldPositionInHeader(fieldKey,lines[0]);
		// if fieldKey had be find in headers
		if (posKey>-1){
			// we create an array with headers-->field and values
			var count = lines.length-1;
			for(i=1;i<lines.length;i++) {
				(function(i) {
					//replace all disturbing elements \n " and \r
					var mesvalues=lines[i].replace(/[\n\r\"]/g,"").split(separator);
					// create json string
					var maList='{"';
					for (j in headers){
						maList+=headers[j]+'" : "'+mesvalues[j]+'" , "';
					}//end for j
					
					// remove last three car ", "
					maList=maList.substring(0, maList.length - 3)+"}";
					
					//convert string to Object
					var result = JSON.parse(maList);
					
					// // we do a single hmset by line with the parameters array
					client.hmset(keyTableName+":"+mesvalues[posKey],result,function(err,replies){});
					count--;
					if (count === 0) {
						client.quit(); // on ferme la connection avant de rendre la main
						var endDate=new Date();
						var duration=(endDate-startDate);
						// should return number of insert keys
						var numberRecord=1;
						cb(numberRecord);
					} //end if count
				})(i);//end function (i)
			} // end for i
		} else {
		// fieldKey are not in the headers return -1
			cb( 'no Headers');
		}
	})//end readfile
}//end function


// save hash element to csv file
// 	insert csv to Redis Hash table keys
//  	var keyname='article:*';
//	var filename=__dirname+'/data2.csv';
//	var countnb=1000;
//	separator='@';
//	hashTocsv(keyname,filename,countnb,cb,);
function hashTocsv(keyTableName, filename, countnb, cb, separator, mIndex){
	var mIndex=(typeof mIndex === "undefined") ?'0':mIndex;	
	var separator=(typeof separator === "undefined") ?';':separator;
	var client = redis.createClient();
	
	// scan all countnb hash matching keyname
	client.send_command('scan', [mIndex,'MATCH',keyTableName, 'COUNT', countnb], function (err, reply) {
		console.log(reply);
		var mIndex=reply[0];
		var myCol=[];
		var colKey=[];
		var index=0;
		var mData=reply[1];
		var monTab=[];
		var mRow=[];
		
		// prepare multi execution 
		for (i=0;i<mData.length;i++){
			monTab.push(['hgetall',mData[i]]);
		}//end for
		
		// get all information about each hash
		client.multi(monTab).exec(function (err, replies) {
			for (var i=0;i<replies.length;i++){
				var mLigne=[];
				var mHash=replies[i];
				var maVal=Object.keys(mHash);
				for (var j=0;j<maVal.length;j++){
					var myKey=maVal[j];
					//defined column order
					if (typeof myCol[myKey] == "undefined") {
						myCol[myKey]=index;
						colKey[index]=myKey;
						index++;
					}
					mLigne[myCol[myKey]]=mHash[myKey];
				}
				maPhrase=mLigne.join(separator);
				mRow.push(maPhrase);
			}; //end for
			
			//prepare title line
			monTitre=colKey.join(separator);
			mRow.unshift(monTitre);
			mstring=mRow.join('\n\r');
			
			//write into a file
			fs.appendFile(filename, mstring, function(err) {
				if(err) {console.log(err);}
				console.log('sauvegarde '+filename);
				cb('Done');
			}); 
		}); // end multi
	});//end client send
}

/************************************ Export *******************************/
exports.csvToHash = csvToHash;
exports.hashTocsv = hashTocsv;