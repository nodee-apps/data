'use strict';

var getObjValue = require('nodee-utils').object.deepGet;

/*
 * Compare 2 SORTED! data feeds (source, destination) by given key
 *
 * @example
 * var result = compare({
        key:'id',
        chunkSize: 2,
        source: sourceItems Array,
        destination: destItems Array,
        ignoreDuplicities: true
    });
*/

module.exports = function compare(keyName, chunkSize, sourceItems, destItems, decideUpdate, ignoreDupliciteKey){
    var sourceKeyName, destKeyName;
    
    if(arguments.length===1){
        var opts = arguments[0];
        
        keyName = opts.keyName || opts.key;
        chunkSize = opts.chunkSize;
        sourceItems = opts.sourceItems || opts.source;
        destItems = opts.destItems || opts.dest || opts.destinationItems || opts.destination;
        decideUpdate = opts.decideUpdate;
        ignoreDupliciteKey = opts.ignoreDupliciteKey || opts.ignoreDuplicities || opts.ignoreDuplicates;
        sourceKeyName = opts.sourceKeyName || opts.sourceKey || keyName;
        destKeyName = opts.destKeyName || opts.destKey || opts.destinationKey || keyName;
    }
    
    sourceKeyName = sourceKeyName || keyName;
    destKeyName = destKeyName || keyName;
    
    decideUpdate = decideUpdate || function(){ return true; };
    if(typeof decideUpdate !== 'function') throw new Error('Argument "decideUpdate" is not Function');
    
    var toRemove = [];
    var toInsert = [];
    var toInsertAfter = [];
    var toUpdate = [];
    var toNextSource = [];
    var toNextDest = [];
    var toCompareSource = [];
    var toCompareDest = [];
    var toIgnore = [];
    var e;
    
    var minSourceId = getObjValue((sourceItems[0] || {}), sourceKeyName);
    var maxSourceId = getObjValue((sourceItems[sourceItems.length-1] || {}), sourceKeyName);
    var minDestId = getObjValue((destItems[0] || {}), destKeyName);
    var maxDestId = getObjValue((destItems[destItems.length-1] || {}), destKeyName);
    var isLastSource = !chunkSize || sourceItems.length < chunkSize;
    var isLastDest = !chunkSize || destItems.length < chunkSize;
    
    var i, nextSourceId, nextDestId, sourceId, destId, dupl;
    
    // filter source items outside of range destIds 
    for(i=0;i<sourceItems.length;i++){
        sourceId = getObjValue(sourceItems[i], sourceKeyName);
        nextSourceId = getObjValue(sourceItems[i+1], sourceKeyName);
        
        if(sourceId < minDestId){
            if(isInvalid(sourceId, nextSourceId)) return isInvalid(sourceId, nextSourceId);
            dupl = isDupliciteSource(sourceId, nextSourceId);
            if(dupl===false) toInsert.push(sourceItems[i]);
            else if(dupl instanceof Error) return dupl;
        }
        else if(sourceId > maxDestId && isLastDest){
            if(isInvalid(sourceId, nextSourceId)) return isInvalid(sourceId, nextSourceId);
            dupl = isDupliciteSource(sourceId, nextSourceId);
            if(dupl===false) toInsertAfter.push(sourceItems[i]);
            else if(dupl instanceof Error) return dupl;
        }
        else if(sourceId > maxDestId){
            toNextSource.push(sourceItems[i]);
        }
        else {
            toCompareSource.push(sourceItems[i]);
        }
    }
    
    // filter dest items outside of range sourceIds 
    for(i=0;i<destItems.length;i++){
        destId = getObjValue(destItems[i], destKeyName);
        
        if(destId < minSourceId)
            toRemove.push(destItems[i]);
        else if(destId > maxSourceId && isLastSource){
            toRemove.push(destItems[i]);
        }
        else if(destId > maxSourceId){
            toNextDest.push(destItems[i]);
        }
        else {
            toCompareDest.push(destItems[i]);
        }
    }
    
    // compare items
    var s = 0;
    var d = 0;
    while(s <= toCompareSource.length && d <= toCompareDest.length){
        sourceId = getObjValue((toCompareSource[s]|| {}), sourceKeyName);
        destId = getObjValue((toCompareDest[d] || {}), destKeyName);
        nextSourceId = getObjValue((toCompareSource[s+1] || {}), sourceKeyName);
        nextDestId = getObjValue((toCompareDest[d+1] || {}), destKeyName);
        
        // ignore undefined id
        if(toCompareSource[s] && sourceId === undefined) {
            s++; // skip this sourceId
            continue;
        }
        // ignore undefined id
        if(toCompareDest[d] && destId === undefined) {
            d++; // skip this destId
            continue;
        }
        
        // check if data are valid
        if(isInvalid(sourceId, nextSourceId, destId, nextDestId)) return isInvalid(sourceId, nextSourceId, destId, nextDestId); 
        
        if(nextSourceId && nextSourceId === sourceId) { // multiple source ids
            if(ignoreDupliciteKey === true || ignoreDupliciteKey === 'source') {
                d--; // repeat for last destId
                toIgnore.push({ sourceId:sourceId });
            }
            else return new Error('Duplicite source ID "' +nextSourceId+ '"');
        }
        else if(nextDestId && nextDestId === destId) { // multiple dest ids
            if(ignoreDupliciteKey === true || ignoreDupliciteKey === 'dest') {
                s--; // repeat for last sourceId
                toIgnore.push({ destId:destId });
            }
            else return new Error('Duplicite destination ID "' +nextDestId+ '"');
        }
        else if(sourceId === destId && sourceId !== undefined && sourceId !== null){ // items matched
            if(decideUpdate(toCompareSource[s], toCompareDest[d]) === true)
                toUpdate.push({ sourceId:sourceId, source:toCompareSource[s], destId:destId ,dest:toCompareDest[d]});
        }
        else if(d === toCompareDest.length){ // end of destItems, mark all source items as to Insert
            e = markToNextMatchOrEnd(sourceKeyName, toCompareSource, toInsert, null, s, true);
            if(e instanceof Error) return e;
            break;
        }
        else if(s === toCompareSource.length){ // end of sourceItems, mark all dest items as to Remove
            markToNextMatchOrEnd(destKeyName, toCompareDest, toRemove, null, d);
            break;
        }
        else if(sourceId > destId){ // mark all dest items with id greater than sourceId as to Remove
            d = markToNextMatchOrEnd(destKeyName, toCompareDest, toRemove, sourceId, d);
            s--; // repeat for last sourceId
        }
        else if(sourceId < destId) { // mark all source items with id lower than destId as to Insert
            s = markToNextMatchOrEnd(sourceKeyName, toCompareSource, toInsert, destId, s, true);
            if(s instanceof Error) return s;
            d--; // repeat for last destId
        }
        
        d++;
        s++;
    }
    
    function markToNextMatchOrEnd(keyName, items, toArray, maxId, index, validateSource){
        for(var i=index;i<items.length;i++){
            var id = getObjValue((items[i]|| {}), keyName);
            var nextId = getObjValue((items[i+1]|| {}), keyName);
            var dupl;
            
            if(validateSource && nextId!==undefined) {
                if(isInvalid(id, nextId)) return isInvalid(id, nextId);
                dupl = isDupliciteSource(id, nextId);
                if(dupl===true) continue;
                else if(dupl) return dupl;
            }
            
            if(maxId===null || maxId===undefined || id < maxId){
                toArray.push(items[i]);
            }
            else return i-1;
        }
        
        return i-1;
    }
    
    function isDupliciteSource(id, nextId){
        if(id===nextId){
            if(ignoreDupliciteKey === true || ignoreDupliciteKey === 'source') {
                toIgnore.push({ sourceId:id });
                return true;
            }
            else return new Error('Duplicite source ID "' +nextId+ '"');
        }
        return false;
    }
    
    return {
        remove: toRemove,
        create: toInsert.concat(toInsertAfter),
        update: toUpdate,
        nextSource: toNextSource,
        nextDest: toNextDest,
        ignore: toIgnore
    };
};

function isInvalid(sourceId, nextSourceId, destId, nextDestId){
    // check if sortable type
    var sourceType = typeof sourceId;
    if(sourceId!==undefined && sourceType!=='number' && sourceType!=='string' && !(sourceId instanceof Date)) return new Error('Bad source type, "' +sourceId+ '" should be number, string or date and is "' +sourceType+ '"');

    // check types equality
    if(nextSourceId && typeof nextSourceId !== sourceType) return new Error('Bad source key type, "' +sourceId+ '" (' + sourceType + ') should be same typeof as "' +nextSourceId+ '" (' + typeof nextSourceId + ')');
    
    // bad sort
    if(nextSourceId && nextSourceId < sourceId) return new Error('Bad source sort, "' +nextSourceId+ '" should be greater than "' +sourceId+ '"');
    
    // also check destination
    if(arguments.length > 2){
        
        // check if sortable type
        var destType = typeof destId;
        if(destId!==undefined && destType!=='number' && destType!=='string' && !(destId instanceof Date)) return new Error('Bad dest type, "' +destId+ '" should be number, string or date and is "' +destType+ '"');

        // check types equality
        if(nextDestId && typeof nextDestId !== destType) return new Error('Bad destination key type, "' +destId+ '" (' + destType + ') should be same typeof as "' +nextDestId+ '" (' + typeof nextDestId + ')');

        // bad sort
        if(nextDestId && nextDestId < destId) return new Error('Bad destination sort, "' +nextDestId+ '" should be greater than "' +destId+ '"');
        
    }    
}