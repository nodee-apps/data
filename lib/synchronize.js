'use strict';

var compareChunk = require('./compare.js'),
    getObjValue = require('nodee-utils').object.deepGet,
    Series = require('nodee-utils').async.Series;

/*
 * Sync destination data with source data
 * 
 * @example:
 * var opts = {
        keyName: 'id',
        ignoreDuplicities: true,
        chunkSize: 5,
        sourceRead: function(i, cb){
            // read next 5 items
            cb(Array sourceItems);
        },
        destRead: function(i, cb){
            // read next 5 items
            cb(err, Array destItems);
        },
        decideUpdate: function(sourceItem, destItem){
            return true;
        },
        onUpdate: function(sourceItem, destItem, next){
            updated.push(sourceItem, destItem);
            next(err);
        },
        onCreate: function(sourceItem, next){
            created.push(sourceItem);
            next(err);
        },
        onRemove: function(destItem, next){
            removed.push(destItem);
            next(err);
        },

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
    });
 * 
 *
 * opts:
 * @param {String} key - property name to match data
 * @param {String} sourceKey - if source property name to match data is different to keyName
 * @param {String} destKey - if target property name to match data is different to keyName
 * @param {Function} sourceRead function(iteration, cb) - cb(err, data) SORTED BY keyName!
 * @param {Function} destRead function(iteration, cb) - cb(err, data) SORTED BY keyName!
 * @param {Number} chunkSize - max items per read, if not set, only one iteration will run
 * @param {Function} decideUpdate function(sourceItem, destItem) - return true if dest need to be updated (default true to all)
 * @param {Boolean} continuousUpdate (default true) decide if update update will run after each chunk comparison
 * @param {Boolean} continuousCreate (default true) decide if update create will run after each chunk comparison, WARNING: created items can broke destRead cont, use it with createdDT filter
 * @param {Boolean} continuousRemove (default true) decide if update remove will run after each chunk comparison, WARNING: removing items can broke destRead cont, use it only if soft remove, or update
 * @param {Function} onUpdate function(sourceItem, destItem, next) - continuous updating while comparing, or run for each item, after comparison
 * @param {Function} onCreate function(sourceItem, next) - run for each item, after comparison
 * @param {Function} onRemove function(destItem, next) - run for each item, after comparison
 * @param {Function} onError function(err) - custom CRUD error handler, default is throw new Error
 * @param {Bool/String} ignoreDupliciteKey bool/string('source'/'dest') - if set, compare will ignore ID duplicity
 * 
 * @param {Function} done function(err, { source, dest, inserted, updated, removed }) - comparison counts result
 * 
 */
module.exports = function synchronize(opts, done){
    var sourceCount =0,
        destCount = 0,
        created = 0,
        updated = 0,
        removed = 0,
        i = 0,
        endOfSource = false,
        endOfDest = false,
        toNextSource = [],
        toNextDest = [],
        toInsert = [],
        toRemove = [],
        toUpdate = [],
        toIgnore = [];
    
    function finish(){
        updateAll(toUpdate, function(){
            createAll(toInsert, function(){
                removeAll(toRemove, function(){
                    done(null, {
                        source: sourceCount,
                        dest: destCount,
                        created: (created || 0) + toInsert.length,
                        updated: (updated || 0) + toUpdate.length,
                        removed: (removed || 0) + toRemove.length,
                        ignored: toIgnore.length
                    });
                });
            });
        });
    }
    
    function sync(i){
        setImmediate(function(){
            readChunk(i, function(err, sourceItems, destItems){
                if(err) return done(err);
                
                sourceCount += sourceItems.length;
                destCount += destItems.length;
                
                var compared = compareChunk({
                    keyName: opts.keyName || opts.key,
                    sourceKeyName: opts.sourceKeyName,
                    destKeyName: opts.destKeyName,
                    chunkSize: opts.chunkSize,
                    sourceItems: toNextSource.concat(sourceItems),
                    destItems: toNextDest.concat(destItems),
                    decideUpdate: opts.decideUpdate,
                    ignoreDupliciteKey: opts.ignoreDupliciteKey || opts.ignoreDuplicities || opts.ignoreDuplicates
                });
                
                if(compared instanceof Error) {
                    return done(compared);
                }
                
                toNextSource = compared.nextSource;
                toNextDest = compared.nextDest;
                toRemove = toRemove.concat(compared.remove);
                toInsert = toInsert.concat(compared.create);
                toUpdate = toUpdate.concat(compared.update);
                toIgnore = toIgnore.concat(compared.ignore);
                i++;
                
                if(endOfSource && endOfDest) finish();
                else partialFinish(function(){
                    sync(i);
                });
            });
        });
    }
    
    function partialFinish(cb){
        updateAll(toUpdate, function(){
            if(opts.continuousUpdate!==false){
                updated += toUpdate.length;
                toUpdate = [];
            }
            
            createAll(toInsert, function(){
                if(opts.continuousCreate!==false){
                    created += toInsert.length;
                    toInsert = [];
                }
                
                removeAll(toRemove, function(){
                    if(opts.continuousRemove!==false){
                        removed += toRemove.length;
                        toRemove = [];
                    }
                    
                    cb();
                    
                }, opts.continuousRemove===false);
            }, opts.continuousCreate===false);
        }, opts.continuousUpdate===false);
    }
    
    function updateAll(items, cb, skip){
        if(skip) return cb();
        
        Series.each(items, function(i, next){
            opts.onUpdate(items[i].source, items[i].dest, next);    
        }, function(err){
            if(opts.onError && err) opts.onError(err);
            else if(err) return done(err);
            else cb();
        });
    }
    
    function createAll(items, cb, skip){
        if(skip) return cb();
        
        Series.each(items, function(i, next){
            opts.onCreate(items[i], next);
        }, function(err){
            if(opts.onError && err) opts.onError(err);
            else if(err) return done(err);
            else cb();
        });
    }
    
    function removeAll(items, cb, skip){
        if(skip) return cb();
        
        Series.each(items, function(i, next){
            opts.onRemove(items[i], next);    
        }, function(err){
            if(opts.onError && err) opts.onError(err);
            else if(err) return done(err);
            else cb();
        });
    }
    
    function readChunk(i, cb){ // cb(err, sourceItems, destItems)
        if(!endOfSource){
            opts.sourceRead(i, function(err, sourceItems){
                if(err) return cb(err);
                if(sourceItems.length < opts.chunkSize || !opts.chunkSize) endOfSource = true;    
                
                if(!endOfDest){
                    opts.destRead(i, function(err, destItems){
                        if(err) return cb(err);
                        if(destItems.length < opts.chunkSize || !opts.chunkSize) endOfDest = true; 
                        cb(null, sourceItems, destItems);
                    });
                }
                else cb(null, sourceItems, []);
            });
        }
        else if(!endOfDest){
            opts.destRead(i, function(err, destItems){
                if(err) return cb(err);
                if(destItems.length < opts.chunkSize || !opts.chunkSize) endOfDest = true; 
                cb(null, [], destItems);
            });
        }
        else cb(null, [], []);
    }
    
    // run sync
    sync(i);
};