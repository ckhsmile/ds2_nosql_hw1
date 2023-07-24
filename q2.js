// TODO:

//var input_tag = 'italian western';

// var cursor = db.ml_tags.aggregate([
//     {$match:{tag: input_tag}},
//     {$lookup:{from:"ml_movies", localField:"movieId", foreignField: "movieId", as:"ml_info"}},
//     {$unwind:"$ml_info"},
//     {$project: {_id:0,title:"$ml_info.title"} }
// ]);

// ml_tags에서 입력한 태그 정보를 가진 movieId 가져오기
var movieIds = db.ml_tags.find({tag: input_tag}, {movieId: 1, _id: 0}).map(function(result) {return result.movieId});

// 각 movieId에 대해 ml_movies 제목 검색
var cursor = db.ml_movies.find({movieId: {$in:movieIds}}, {title: 1, _id: 0});

while (cursor.hasNext()) {
    var doc = cursor.next();
    print(doc.title);
}

