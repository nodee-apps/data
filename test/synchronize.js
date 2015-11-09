var synchronize = require('../lib/synchronize.js'),
    assert = require('assert');

var updated = [];
var created = [];
var removed = [];
var id = 1;
var opts = {
    keyName: 'id',
    sourceRead: function(i, cb){
        id = 2;
        if(i===0) cb(null, [
            { id: id+1 },
            { id: id+2 },
            { id: id+3 },
            { id: id+4 },
            { id: id+5 },
            { id: id+5 }
        ]);
        else cb(null, []);
    },
    destRead: function(i, cb){
        if(i===0) cb(null, [
            {id:1},
            {id:2},
            //{id:3},
            {id:4},
            {id:5}
        ]);
        else cb(null, []);
    },
    chunkSize: 5,
    decideUpdate: function(sourceItem, destItem){
        return true;
    },
    onUpdate: function(sourceItem, destItem, next){
        updated.push(sourceItem, destItem);
        next();
    },
    onCreate: function(sourceItem, next){
        created.push(sourceItem);
        next();
    },
    onRemove: function(destItem, next){
        removed.push(destItem);
        next();
    },
    ignoreDuplicities: true,
    onError: function(err){
        throw err;    
    }
};

synchronize(opts, function(err, summary){
    if(err) throw err;
    assert.deepEqual(summary, { 
        source: 6,
        dest: 4,
        created: 3,
        updated: 2,
        removed: 2,
        ignored: 1 
    });
    
    assert.deepEqual(updated, [ { id: 4 }, { id: 4 }, { id: 5 }, { id: 5 } ]);
    assert.deepEqual(created, [ { id: 3 }, { id: 6 }, { id: 7 } ]);
    assert.deepEqual(removed, [ { id: 1 }, { id: 2 } ]);
    
    console.log('synchronize - OK');
});