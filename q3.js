// TODO:

//var input_title = 'Interstellar (2014)';

// var cursor = db.ml_movies.aggregate([
//     {$match:{title:input_title}},
//     {$lookup:{from:'ml_ratings', localField:'movieId', foreignField:'movieId', as:'result'}},
//     {$unwind:'$result'},
//     {$group:{_id:null, sum:{$sum:'$result.rating'}, cnt:{$sum:1}}}
// ]);


// if (cursor.hasNext()) {
//     var result = cursor.next();
//     sum = result.sum;
//     cnt = result.cnt;
// }


var movieIds = db.ml_movies.find({title:input_title},{_id:0, movieId:1}).map(function(result) {return result.movieId});

var cnt = db.ml_ratings.find({movieId:{$in:movieIds}}).count()

var sum = db.ml_ratings.aggregate([
                                    {$match:{movieId:{$in:movieIds}}},
                                    {$group:{_id:null, sum:{$sum:'$rating'}}}
                                ]).map(function(result) {return result.sum});

var avg = (sum / cnt).toFixed(3);
print(avg);



