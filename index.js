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
                console.log('error getting key');
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
var errors = [];
//create a process for processing id's
mapIds= function(docs, callback) {
    if(!docs || docs.length == 0){
        return
    }
    var current= -1;

    var map= function(err){
        if (err){
            errors.push(err);
        }
        current++;
        if (current == docs.length) {
         return callback();
        }
        var type;
        if(docs[current]["collection"]){
            type= String(docs[current]["collection"]).charAt(0);
        }
        sequence.next(type,function(id) {
            db.put(docs[current]["_id"], id, map);
        });

    };
    map();
};
//Function for retrieving the mapping and
swapIds= function(docs, callback){
    if(!docs || docs.length == 0){
        return
    }
    var current= -1;
    swap=function(){
        current++;
        if (current == docs.length) {
            return callback(docs);
        }
        var props=[];
        for (var property in docs[current]) {
            var prop = String(property);
            if (prop === "fromInstanceId") {
                continue;
            }
            if (re.test(prop)) {
                if (docs[current][prop]) {
                    props.push(prop);
                }
            }
        }
        if (props.length == 0){
            return;
        }
        var curProp= -1
        var lookup =function(err, value){
            if (err) {
                errors.push("Error on Property:  " + props[curProp]);
                errors.push(err);
            }
            if(value){
                docs[current][props[curProp]] = value;
            }
            curProp++;
            if(curProp == props.length){
                return swap();
            }

            db.get(docs[current][props[curProp]], lookup);

        };
        lookup();

    };
    swap();
};


var groups = ["group-drc_eddata_2015"]; // list of groups to transform

var doGroup = function() {
    var groupName = groups.pop();

    console.log('\n\n Migrating Group: ' + groupName);

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
                    console.log('Mapping Complete');
                    console.log('That took ' + (parseInt((new Date()).getTime() - timeBegin) / 1000));
                    console.log('Initiating Swapping');
                    lastId = '';
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
                    console.log(rows.length);
                    console.log('Swapping Complete');
                    console.log('That took ' + (parseInt((new Date()).getTime() - timeBegin) / 1000));
                    console.log('Group Errors:');
                    errors.forEach(function(err){console.log(err);});
                    errors=[];
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
                    process.nextTick(step2);
                    /*
                    unirest.put('http://localhost:5984/')
                        .headers(JSON_HEADERS)
                        .auth(process.env.T_ADMIN, process.env.T_PASS)
                        .end(function (response) {

                            if (!response.error) {
                                console.log('database (' + localGroupPath + ') created');
                            }

                            unirest.post('http://localhost:5984/' + groupName + '/_bulk_docs')
                                .headers(JSON_HEADERS)
                                .auth(process.env.T_ADMIN, process.env.T_PASS)
                                .send({docs:docs})
                                .end(function (response) {
                                    if(!response.body.rows){
                                        console.log('migrated' + docs.length + 'documents');
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

                        });
                        */
                });
                // do work above here, call doOne when done a step
            });

    };
};




