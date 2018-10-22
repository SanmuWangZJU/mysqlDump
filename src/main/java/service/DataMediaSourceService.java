package service;

import model.MediaSource;

import javax.sql.DataSource;

public interface DataMediaSourceService {
    DataSource getDataSource(MediaSource mediaSource);
}
