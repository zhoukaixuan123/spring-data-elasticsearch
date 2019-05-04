package com.leyou.es.pojo;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface ItemRepostory extends ElasticsearchRepository<Item,Long> {

    List<Item>  findByPriceBetween(Double begin,Double end);

}

