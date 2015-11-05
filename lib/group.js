'use strict';

var getObjValue = require('enterprise-utils').object.deepGet,
    Series = require('enterprise-utils').async.Series;

/*
 * Sync destination data with source data
 * 
 * @example:
 * var opts = {
        keyName: 'id',
        chunkSize: 5,
        sourceRead: function(i, cb){
            // read next 5 items
            cb(Array sourceItems);
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

        assert.deepEqual(summary, { 
            source: 11, 
            grouped: 4
        });
    });
 *
 * opts:
 * @param {String} key property name to match data
 * @param {Function} sourceRead function(iteration, cb) - cb(data) SORTED BY keyName!
 * @param {Number} chunkSize - max items per read, if not set, only one iteration will run
 * @param {Function} decideGroup function(sourceItem, destItem) - return true if dest need to be updated (default true to all)
 * @param {Boolean} continuousGroup - (default true) continuous group, decide if group func will run after each chunk grouping
 * @param {Function} onGroup - function(groupedItem, next) - continuous updating while comparing, or run for each item, after comparison
 * @param {Function} onError function(err) - custom CRUD error handler, default is throw new Error
 * @param {Boolean} ignoreUndefined - what to do if key is missing, default is to throw Error
 * 
 * @param {Function} done function(err, { source, dest, inserted, updated, removed }) - comparison counts result
 */
module.exports = function group(opts, done){
    var sourceCount =0,
        grouped = 0,
        i = 0,
        endOfSource = false,
        toNextGroup = {},
        toGroup = {};
    
    function finish(){
        groupAll(toGroup, function(){
            done(null, {
                source: sourceCount,
                grouped: grouped
            });
        });
    }
    
    function grouping(i){
        setImmediate(function(){
            readChunk(i, function(sourceItems){
                sourceCount += sourceItems.length;
                
                var groupResult = groupChunk({
                    keyName: opts.keyName || opts.key,
                    chunkSize: opts.chunkSize,
                    sourceItems: sourceItems || opts.source,
                    groups: toNextGroup,
                    decideGroup: opts.decideGroup,
                    ignoreUndefined: opts.ignoreUndefined
                });
                
                if(groupResult instanceof Error) {
                    done(groupResult);
                    return;
                }
                
                toNextGroup = groupResult.toNextGroup;
                for(var gId in groupResult.groups){
                    toGroup[gId] = groupResult.groups[gId];
                }
                i++;
                
                if(endOfSource) finish();
                else if(opts.continuousGroup !== false) groupAll(toGroup, function(){
                    toGroup = {};
                    grouping(i);
                });
                else grouping(i);
            });
        });
    }
    
    function groupAll(groups, cb){
        grouped += Object.keys(toGroup).length;
        
        Series.each(groups, function(gId, next){
            opts.onGroup(gId, groups[gId], next);    
        }, function(err){
            if(opts.onError && err) opts.onError(err);
            else if(err) throw err;
            else cb();
        });
    }
    
    function readChunk(i, cb){ // cb(sourceItems, destItems)
        if(!endOfSource){
            opts.sourceRead(i, function(sourceItems){
                if(sourceItems.length < opts.chunkSize || !opts.chunkSize) endOfSource = true;    
                cb(sourceItems);
            });
        }
        else cb([]);
    }
    
    // run grouping
    grouping(i);
};

function groupChunk(keyName, chunkSize, sourceItems, groups, decideGroup, ignoreUndefined){
    if(arguments.length===1){
        var opts = arguments[0];
        
        keyName = opts.keyName || opts.key;
        chunkSize = opts.chunkSize;
        sourceItems = opts.sourceItems;
        groups = opts.groups;
        ignoreUndefined = opts.ignoreUndefined;
        decideGroup = opts.decideGroup || function(){ return true; };
    }
    
    groups = groups || {};
    var isLastSource = !chunkSize || sourceItems.length < chunkSize;
    var toNextGroup = {};
    
    // group items
    var s = 0;
    var sourceId, nextSourceId, prevSourceId;
    while(s < sourceItems.length){
        sourceId = getObjValue((sourceItems[s]|| {}), keyName);
        nextSourceId = getObjValue((sourceItems[s+1] || {}), keyName);
        prevSourceId = getObjValue((sourceItems[s-1] || {}), keyName);
        
        // bad sort
        if(nextSourceId < sourceId) return new Error('Bad source sort "' +nextSourceId+ '" should be greater than "' +sourceId+ '"');
        
        // sourceId undefined
        if(sourceId===null || sourceId===undefined) {
            if(!ignoreUndefined) return new Error('Undefined sourceId on item index "' +s+ '"');
        }
        else if(s===0){ // first item
            groups[sourceId] = groups[sourceId] || [];
            groups[sourceId].push(sourceItems[s]);
        }
        else if(prevSourceId === sourceId) { // duplicite ids - mark as toGroup
            if(decideGroup(sourceItems[s], groups[sourceId]) === true) {
                groups[sourceId].push(sourceItems[s]);
            }
        }
        else { // create new group
            groups[sourceId] = [sourceItems[s]];
        }
        
        if(s === sourceItems.length-1){ // end of sourceItems, let last group to next time if its not lastChunk
            if(!isLastSource && !(sourceId===null || sourceId===undefined)) {
                toNextGroup[sourceId] = groups[sourceId];
                delete groups[sourceId];
            }
            
            break;
        }
        s++;
    }
    
    return {
        toNextGroup: toNextGroup,
        groups: groups
    };   
}