package com.zhonggu.bluevalley.iquant.database.rdbopt.interceptor;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.ibatis.executor.resultset.DefaultResultSetHandler;
import org.apache.ibatis.executor.resultset.ResultSetHandler;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * create by spruce on 18-4-17
 **/
@Intercepts({@Signature(
        type= ResultSetHandler.class,
        method = "handleResultSets",
        args = {Statement.class})})
public class ColumnToRowInterceptor implements Interceptor{

    private Map<String,Class> config;

    public ColumnToRowInterceptor(Map<String,Class> config) {
        this.config = config;
    }

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        Object target = invocation.getTarget();
        if(target instanceof DefaultResultSetHandler){
            DefaultResultSetHandler handler = (DefaultResultSetHandler) target;
            MappedStatement statement = this.getMappedStatement(handler);
            String methodId = statement.getId();
            // 判定拦截的方法
            if(config.containsKey(methodId)){
                Statement stmt = (Statement) invocation.getArgs()[0];
                return rehandleResultSet(stmt.getResultSet(),config.get(methodId));
            }
        }
        return invocation.proceed();
    }

    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {
    }


    private MappedStatement getMappedStatement(DefaultResultSetHandler defaultResultSetHandler) throws NoSuchFieldException {
        Field field = defaultResultSetHandler.getClass().getDeclaredField("mappedStatement");
        field.setAccessible(true);
        Object value = null;
        try {
            value = field.get(defaultResultSetHandler);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (MappedStatement)value;
    }

    private Object rehandleResultSet(ResultSet resultSet,Class resultType) {
        if(resultSet != null){
            // 获取结果集转换成特定格式map
            List<Object> list = new ArrayList<>();
            Map<String,String> map = new HashMap<>();
            try {
                while (resultSet.next()){
                    String key = resultSet.getString(1);
                    String value = resultSet.getString(2);
                    map.put(key,value);
                }
                Class clazz = resultType.getDeclaredClasses()[0];
                Object target = clazz.newInstance();
                BeanUtils.populate(target,map);
                list.add(target);
                return list;
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                closeResultSet(resultSet);
            }
        }
        return null;
    }

    private void closeResultSet(ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
