var group = require('../lib/group.js'),
    assert = require('assert');

var groups = {};

var opts = {
    keyName: 't',
    chunkSize: 5,
    sourceRead: function(i, cb){
        //console.log('reading source i=' + i);
        if(i===0) cb(null, [
            { id:'5', t:'a'},
            { id:'61704', t:'a'},
            { id:'61705', t:'a'},
            { id:'61913', t:'a'},
            { id:'61914', t:'a'},
            { id:'61925', t:'b'},
            { id:'61989', t:'b'},
            { id:'61991', t:'c'},
            { id:'62071', t:'c'},
            { id:'62072', t:'c'},
            { id:'62080', t:'d'}
        ]);
        
        if(i>0) cb(null, []);
    },
    onGroup: function(key, group, next){
        groups[key] = group;
        next();
    },
    onError: function(err){
        throw err;   
    }
};

group(opts, function(err, summary){
    if(err) throw err;
    
    assert.deepEqual(summary, { source: 11, grouped: 4 });
    assert.deepEqual(groups, { 
        a:[ 
            { id: '5', t: 'a' },
            { id: '61704', t: 'a' },
            { id: '61705', t: 'a' },
            { id: '61913', t: 'a' },
            { id: '61914', t: 'a' } 
        ],
        b:[ 
            { id: '61925', t: 'b' }, 
            { id: '61989', t: 'b' } 
        ],
        c:[   
            { id: '61991', t: 'c' },
            { id: '62071', t: 'c' },
            { id: '62072', t: 'c' } 
        ],
        d:[ 
            { id: '62080', t: 'd' } 
        ]
    });
    console.log('group - OK');
});