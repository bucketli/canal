package com.alibaba.otter.canal.migration.model;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;
import java.util.Properties;

/**
 * @author bucketli 2019/7/30 3:43 PM
 * @since 1.1.3
 **/
public class DataSourceConfig implements Serializable {

    private static final long serialVersionUID = 7889678499141691020L;
    private final String      username;
    private final String      password;
    private final String      url;
    private final DBType      type;
    private final String      encode;
    private final Properties  properties;

    public DataSourceConfig(String username, String password, String url, DBType type, String encode,
                            Properties properties){
        this.username = username;
        this.password = password;
        this.url = url;
        this.type = type;
        this.encode = encode;
        this.properties = properties;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getUrl() {
        return url;
    }

    public DBType getType() {
        return type;
    }

    public String getEncode() {
        return encode;
    }

    public Properties getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataSourceConfig that = (DataSourceConfig) o;

        if (username != null ? !username.equals(that.username) : that.username != null) return false;
        if (password != null ? !password.equals(that.password) : that.password != null) return false;
        if (url != null ? !url.equals(that.url) : that.url != null) return false;
        if (type != that.type) return false;
        if (encode != null ? !encode.equals(that.encode) : that.encode != null) return false;
        return properties != null ? properties.equals(that.properties) : that.properties == null;
    }

    @Override
    public int hashCode() {
        int result = username != null ? username.hashCode() : 0;
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + (url != null ? url.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (encode != null ? encode.hashCode() : 0);
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }
}
