// TODO:

/*
db.ml_tags.aggregate([
    {$match:{tag: input_tag}},
    {$lookup:{from:"ml_movies", localField:"movieId", foreignField: "movieId", as:"ml_info"}},
    {$unwind:"$ml_info"},
    {$project: {_id:0,title:"$ml_info.title"} }
]).explain('executionStats')

-> 4.2X 버전에서는 aggregate() 뒤에 explain 바로 호출 불가
*/

// Q2. tag를 기준으로 movieId를 추출할것이기 때문에 tag를 Index key로 설정
db.ml_tags.createIndex({tag:1})
{
        "createdCollectionAutomatically" : false,
        "numIndexesBefore" : 1,
        "numIndexesAfter" : 2,
        "ok" : 1
}

// Q2. ml_tags collection에서 검색하고자 하는 tag가 입력된 movieId를 추출하고, 
// 추출된 movieId를 기준으로 ml_movies collection에서 tilte을 찾아야 하므로
// ml_movies collection의 search key가 되는 movieId를 Index Key로 설정
db.ml_movies.createIndex({movieId:1})
{
        "createdCollectionAutomatically" : false,
        "numIndexesBefore" : 1,
        "numIndexesAfter" : 2,
        "ok" : 1
}

// Q3. ml_movies collection에서 영화 title로 movieId를 검색 후 
// ml_ratings collection에서 movieId를 search key로 하여 raing을 추출할것이기 때문에
// ml_movies에는 title을, ml_ratings에는 movieId를 index로 설정
db.ml_movies.createIndex({title:1})
{
        "createdCollectionAutomatically" : false,
        "numIndexesBefore" : 2,
        "numIndexesAfter" : 3,
        "ok" : 1
}

db.ml_ratings.createIndex({movieId:1})
// 인덱스 생성 시간이 오래 걸림??

// Q4. 특정 고객에 대한 평점 성향 파악 위해 input_userid 기준으로 검색해야 하기 때문에 userId를 Index Key로 추가. 
db.ml_ratings.createIndex({userId:1})
{
    "createdCollectionAutomatically" : false,
    "numIndexesBefore" : 2,
    "numIndexesAfter" : 3,
    "ok" : 1
}


db.runCommand(
    {
      explain: {
        aggregate: "ml_tags",
        pipeline: [
           {$match:{tag: "bittersweet"}},
            {$lookup:{from:"ml_movies", localField:"movieId", foreignField: "movieId", as:"ml_info"}},
            {$unwind:"$ml_info"},
            {$project: {_id:0,title:"$ml_info.title"}}
        ],
        cursor: {}
      },
      verbosity: "executionStats"
    }
 )

/*
{
        "stages" : [
                {
                        "$cursor" : {
                                "query" : {
                                        "tag" : "bittersweet"
                                },
                                "fields" : {
                                        "ml_info.title" : 1,
                                        "movieId" : 1,
                                        "_id" : 0
                                },
                                "queryPlanner" : {
                                        "plannerVersion" : 1,
                                        "namespace" : "movielens.ml_tags",
                                        "indexFilterSet" : false,
                                        "parsedQuery" : {
                                                "tag" : {
                                                        "$eq" : "bittersweet"
                                                }
                                        },
                                        "queryHash" : "C9D72EBE",
                                        "planCacheKey" : "1AB72DA9",
                                        "winningPlan" : {
                                                "stage" : "FETCH",
                                                "inputStage" : {
                                                        "stage" : "IXSCAN",
                                                        "keyPattern" : {
                                                                "tag" : 1
                                                        },
                                                        "indexName" : "tag_1",
                                                        "isMultiKey" : false,
                                                        "multiKeyPaths" : {
                                                                "tag" : [ ]
                                                        },
                                                        "isUnique" : false,
                                                        "isSparse" : false,
                                                        "isPartial" : false,
                                                        "indexVersion" : 2,
                                                        "direction" : "forward",
                                                        "indexBounds" : {
                                                                "tag" : [
                                                                        "[\"bittersweet\", \"bittersweet\"]"
                                                                ]
                                                        }
                                                }
                                        },
                                        "rejectedPlans" : [ ]
                                },
                                "executionStats" : {
                                        "executionSuccess" : true,
                                        "nReturned" : 2044,
                                        "executionTimeMillis" : 164,
                                        "totalKeysExamined" : 2044,
                                        "totalDocsExamined" : 2044,
                                        "executionStages" : {
                                                "stage" : "FETCH",
                                                "nReturned" : 2044,
                                                "executionTimeMillisEstimate" : 0,
                                                "works" : 2045,
                                                "advanced" : 2044,
                                                "needTime" : 0,
                                                "needYield" : 0,
                                                "saveState" : 16,
                                                "restoreState" : 16,
                                                "isEOF" : 1,
                                                "docsExamined" : 2044,
                                                "alreadyHasObj" : 0,
                                                "inputStage" : {
                                                        "stage" : "IXSCAN",
                                                        "nReturned" : 2044,
                                                        "executionTimeMillisEstimate" : 0,
                                                        "works" : 2045,
                                                        "advanced" : 2044,
                                                        "needTime" : 0,
                                                        "needYield" : 0,
                                                        "saveState" : 16,
                                                        "restoreState" : 16,
                                                        "isEOF" : 1,
                                                        "keyPattern" : {
                                                                "tag" : 1
                                                        },
                                                        "indexName" : "tag_1",
                                                        "isMultiKey" : false,
                                                        "multiKeyPaths" : {
                                                                "tag" : [ ]
                                                        },
                                                        "isUnique" : false,
                                                        "isSparse" : false,
                                                        "isPartial" : false,
                                                        "indexVersion" : 2,
                                                        "direction" : "forward",
                                                        "indexBounds" : {
                                                                "tag" : [
                                                                        "[\"bittersweet\", \"bittersweet\"]"
                                                                ]
                                                        },
                                                        "keysExamined" : 2044,
                                                        "seeks" : 1,
                                                        "dupsTested" : 0,
                                                        "dupsDropped" : 0
                                                }
                                        }
                                }
                        }
                },
                {
                        "$lookup" : {
                                "from" : "ml_movies",
                                "as" : "ml_info",
                                "localField" : "movieId",
                                "foreignField" : "movieId",
                                "unwinding" : {
                                        "preserveNullAndEmptyArrays" : false
                                }
                        }
                },
                {
                        "$project" : {
                                "_id" : false,
                                "title" : "$ml_info.title"
                        }
                }
        ],
        "serverInfo" : {
                "host" : "DESKTOP-NPD51MJ",
                "port" : 27017,
                "version" : "4.2.24",
                "gitVersion" : "5e4ec1d24431fcdd28b579a024c5c801b8cde4e2"
        },
        "ok" : 1
}

*/


// MongoDB 4.2 버전부터 $lookup의 pipeline을 사용해서 해당 필드에 대한 인덱스를 이용하는 것이 가능해졌습니다. 그러나 explain()를 이용하여 aggregate를 실행하더라도, $lookup 내부에서 인덱스가 어떻게 사용되었는지에 대한 자세한 정보를 확인하는 것은 어렵습니다.
// 그렇지만 $lookup 연산이 정상적으로 실행된다면, foreignField로 지정된 필드에 인덱스가 존재하면 쿼리 최적화를 위해 이를 사용하게 될 것입니다.
// 더 자세한 정보를 얻기 위해서는 MongoDB의 로깅 레벨을 조정하거나 MongoDB의 소스 코드를 직접 살펴봐야 할 수도 있습니다.
// MongoDB 4.4 버전부터는 $lookup 및 $graphLookup 단계에서 각각의 pipeline에 대한 explain 정보를 포함한 자세한 정보를 얻을 수 있습니다. 만약 사용 가능하다면, MongoDB 버전을 업그레이드하는 것을 고려해 보시기 바랍니다.
