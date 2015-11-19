package org.elasticsearch.es4sql.domain.hints;

import java.util.ArrayList;
import java.util.List;


public enum HintType
{
    HASH_WITH_TERMS_FILTER,
    JOIN_LIMIT,
    USE_NESTED_LOOPS,
    NL_MULTISEARCH_SIZE,
    USE_SCROLL;

}
