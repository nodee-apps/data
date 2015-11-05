var compare = require('../lib/compare.js'),
    assert = require('assert');

var sourceItems = [
    { id:0 },
    { id:1 }, 
    { id:1 }, // ignore
    { id:2 },
    { id:3 },
    { id:4 },
    { id:4 }, // ignore
    { id:8 }, // create
    { id:9 }, // ignore
    { id:9 }  // create
];

var destItems = [
    //{ id:1 }, // create
    { id:2 }, // update
    //{ id:3 }, // create
    { id:4 }, // update
    { id:5 }, // remove
    { id:6 }, // remove
    { id:70 } // create
];

var resultError = compare({
    key:'id',
    chunkSize: 2,
    source: sourceItems,
    destination: destItems
});
assert.ok(resultError.message, 'Duplicite source ID "1"');

var resultOk = compare({
    key:'id',
    chunkSize: 2,
    source: sourceItems,
    destination: destItems,
    ignoreDuplicities: true
});

assert.deepEqual(resultOk, { 
    remove: [ { id: 5 }, { id: 6 } ],
    create: [ { id: 0 }, { id: 1 }, { id: 3 }, { id: 8 }, { id: 9 } ],
    update: [ { sourceId: 2, source: { id:2 }, destId: 2, dest: { id:2 } },
              { sourceId: 4, source: { id:4 }, destId: 4, dest: { id:4 } } ],
    nextSource: [],
    nextDest: [ { id: 70 } ],
    ignore: [ { sourceId: 1 }, { sourceId: 4 }, { sourceId: 9 } ] 
});

console.log('compare - OK');