package com.funcs;

import java.io.Serializable;

/**
 * Created by zhangweimin on 17/5/15.
 */
public interface OnlineAggregationOperation extends Serializable {
    /**
     * @param cols: the cols which be selected or count() or sum() and so on
     * @param specArgs:extend for special use
     * @return type of return value is Object
     * */
    Object exec(String query);
    /**
     * the class should save the exec result
     * and user can use showResult to show in
     * proper format
     * */
    void showResult();
}
