jq -r '[(
    to_entries[] | 
    {   "id":.key, 
        "snippet":"\(.value.title)\n \(.value.overview) \n\(.value.genres | map(.name) | join("\n ")) \n\(.value.cast[:3] | map(.name) | join("\n "))",
        "tags" : ([(.value.genres | map(.name)), (.value.cast[:3] | map(.name))] | flatten(1)),
        "properties" : { 
            "snippet":.value.overview,
            "publication_date":"\(.value.release_date)T00:00:00Z",
            "backdrop_path":(.value.backdrop_path // ""),
            "poster_path":(.value.poster_path // ""),
            "title": .value.title,
            "original_title":.value.original_title,
            "popularity": .value.popularity,
            "vote_average": .value.vote_average,    
            "vote_count": .value.vote_count,
            "language":.value.original_language,
            "imdb_id":(.value.imdb_id // ""),
            "runtime":.value.runtime,
            "budget": .value.budget,
            "revenue": .value.revenue,
            "genres": .value.genres
        } 
    } 
    )]' < $1    
