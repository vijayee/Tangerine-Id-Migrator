var unirest = require('unirest');
var levelup = require('levelup');
var db = levelup('./mappingdb');
var Sequence = require('base62sequence');
var sequencer;
var re= /id$/ig;

var JSON_HEADERS = {
    'Accept'       : 'application/json',
    'Content-Type' : 'application/json'
};

var CHUNK_SIZE = 500;
// check for needed environment variables
if (!(process.env.T_ADMIN && process.env.T_PASS)) {
    console.log('Please set T_ADMIN and T_PASS environment variables.');
    process.exit(1);
}
//use a sequence server instead  to get id's
if (!process.env.Sequence_Server){
    console.log('Please set Sequence_Server environment variables.');
    process.exit(1);
}
//Create a way to generate Sequences from the server
sequencer= function(server, db) {
    this.prefix = null;
    if (!server || !db) {
        return null;
    }
    this.server = server;
    this.db = db;
}
sequencer.prototype.getPrefix= function( callback){
        unirest.post(this.server + '/prefix')
            .headers(JSON_HEADERS)
            .end(function (response) {
                if (!response.body || !response.body.prefix) {
                    return callback(null);
                } else {
                    this.prefix = response.body.prefix;
                    return callback(this.prefix);

                }

            });

};

sequencer.prototype.next = function (type, callback) {
            var nextId;
    unirest.post(this.server+'/id')
        .headers(JSON_HEADERS)
        .send({type: type, db: this.db, clientPrefix: this.prefix})
        .end(function(response){
            if(!response.body || !response.body.ID) {
                nextId= null;
                return callback(nextId);
            } else{
                nextId= response.body.ID;
                return callback(String(this.prefix).concat(type,"-",nextId));
            }



        });
};



var sequence = new sequencer(process.env.Sequence_Server,"china");
sequence.getPrefix(function(prefix){
    console.log("This machine's prefix: " + prefix);
    doGroup();

});

//create a process for processing id's
mapIds= function(docs, callback) {
    console.log("Creating Mappings");
    var mappings = 0;
    var done= docs.length;
    docs.forEach(function(doc){
        var type;
        if(doc["collection"]){
           type= String(doc["collection"]).charAt(0);
        }
        sequence.next(type,function(id){
            db.put(doc["_id"], id, function(err){ //open the box
                if (err){
                    console.log(err);
                }
                mappings++;
                if(mappings >= done){
                    if(callback) {
                        callback();
                    }
                }
            });
        });

    });
};
swapIds= function(docs, callback){
    console.log("Swapping Ids");
    var swaps= 0;
    var done= docs.length
    for(var i=0; i < docs.length; i++){
        var doc= docs[i];
        index= String(i);
        var props=[];
        for (var property in doc) {
            var prop = String(property);
            if (prop === "fromInstanceId") {
                continue;
            }
            if (re.test(prop)) {
                props.push(prop)
            }
        }
        var found= 0;
        var finds= props.length;
        props.forEach(function(prop){
            db.get(doc[prop], function (err, value) {
                found++;
                if (err) {
                    console.log(err);
                }
                docs[Number(index)][prop] = value;
                if (found >= finds) {
                    console.log('finds: ' + finds);
                    swaps++;
                }
                if (swaps >= done) {
                    console.log('swaps: ' + swaps);
                    console.log('done: ' + done);
                    process.nextTick(function () {
                        return callback(docs);
                    });
                }

            });
        });

    }
};


var groups = ["group-drc_eddata_2015"]; // list of groups to transform

var doGroup = function() {
    var groupName = groups.pop();

    console.log('\n\n' + groupName);

    var docCount = 0;
    var lastId = '';

    var beforeDocSize = 0;
    var afterDocSize = 0;

    var timeBegin = (new Date()).getTime();
    var step1 = function() { //cut a hole in the box

        var limit;
        if (lastId === '') {
            limit = CHUNK_SIZE;
        } else {
            limit = CHUNK_SIZE + 1;
        }

        var url = 'http://localhost:5984/' + groupName + '/_all_docs?include_docs=true&limit=' + limit + '&startkey=' + JSON.stringify(lastId);
        unirest.get(url)
            .headers(JSON_HEADERS)
            .auth(process.env.T_ADMIN, process.env.T_PASS)
            .end(function (response) {

                if (!response.body.rows) {
                    return;
                }
                var rows = response.body.rows;
                console.log( 'fetched ' + rows.length + ' rows, lastId: ' + lastId );

                if (lastId !== '') {
                    rows.shift(); // throw out the first document if we used a startkey
                }
                // exit if we didn't get any new documents
                var onlyGotTheStartDoc = rows.length === 1;
                var didntGetAnything = rows.length === 0;
                if (onlyGotTheStartDoc || didntGetAnything) {
                    console.log('All done');
                    console.log('Old size: ' + beforeDocSize);
                    console.log('New size: ' + afterDocSize + ' (' + parseInt((afterDocSize / beforeDocSize) * 100) + '%)');
                    console.log('That took ' + (parseInt((new Date()).getTime() - timeBegin) / 1000));
                    return step2();
                }

                // count how many documents we've seen so far
                docCount = docCount + rows.length;

                // set us up for next loop
                lastId = response.body.rows[response.body.rows.length - 1].id;

                /*
                 * Do real work below here
                 */
                var docs = rows
                    .filter(function (el) {
                        return el.id.substring(0, 8) !== '_design/';
                    }) // axe design docs
                    .map(function (row) {
                        return row.doc;
                    });
                beforeDocSize += JSON.stringify(docs).length;

                mapIds(docs,step1);
                // do work above here, call doOne when done a step
            });

    };

    step1();

    var step2 = function() { //Open the box
        var lastId = '';
        var limit;
        if (lastId === '') {
            limit = CHUNK_SIZE;
        } else {
            limit = CHUNK_SIZE + 1;
        }
        var url = 'http://localhost:5984/' + groupName + '/_all_docs?include_docs=true&limit=' + limit + '&startkey=' + JSON.stringify(lastId);
        unirest.get(url)
            .headers(JSON_HEADERS)
            .auth(process.env.T_ADMIN, process.env.T_PASS)
            .end(function (response) {
                if (!response.body.rows) {
                    return;
                }
                var rows = response.body.rows;
                console.log( 'fetched ' + rows.length + ' rows, lastId: ' + lastId );

                if (lastId !== '') {
                    rows.shift(); // throw out the first document if we used a startkey
                }

                // exit if we didn't get any new documents
                var onlyGotTheStartDoc = rows.length === 1;
                var didntGetAnything = rows.length === 0;
                if (onlyGotTheStartDoc || didntGetAnything) {
                    console.log('All done');
                    console.log('Old size: ' + beforeDocSize);
                    console.log('New size: ' + afterDocSize + ' (' + parseInt((afterDocSize / beforeDocSize) * 100) + '%)');
                    console.log('That took ' + (parseInt((new Date()).getTime() - timeBegin) / 1000));
                    return doGroup();
                }

                // count how many documents we've seen so far
                docCount = docCount + rows.length;

                // set us up for next loop
                lastId = response.body.rows[response.body.rows.length - 1].id;

                /*
                 * Do real work below here
                 */
                var docs = rows
                    .filter(function (el) {
                        return el.id.substring(0, 8) !== '_design/';
                    }) // axe design docs
                    .map(function (row) {
                        return row.doc;
                    });
                swapIds(docs,function(docs){
                    process.nextTick(step2); // clear the stack
                    /*
                    unirest.put('http://localhost:5984/')
                        .headers(JSON_HEADERS)
                        .auth(process.env.T_ADMIN, process.env.T_PASS)
                        .end(function (response) {

                            if (!response.error) {
                                console.log('database ('+localGroupPath+') created');
                            }

                            unirest.post('http://localhost:5984/' + groupName + '/_bulk_docs')
                                .headers(JSON_HEADERS)
                                .auth(process.env.T_ADMIN, process.env.T_PASS)
                                .send({docs:docs})
                                .end(function (response) {
                                    if(!response.body.rows){
                                        console.log('migrated' + docs.length + 'documents');
                                        console.log('top id: ' + docs[0]._id);
                                        return;
                                    }

                                     console.log("write errors: " +
                                     response.body
                                     .rows
                                     .map(function(el){return el.error;})
                                     .filter(function(el){return el;})
                                     .length)

                                    process.nextTick(step2); // clear the stack

                                });

                        });*/
                });
                // do work above here, call doOne when done a step
            });

    };
};




