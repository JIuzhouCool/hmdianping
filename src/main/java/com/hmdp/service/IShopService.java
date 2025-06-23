package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.baomidou.mybatisplus.extension.service.IService;


public interface IShopService extends IService<Shop> {
    /**
     * 根据id查询商铺，并加上缓存
     * @param id
     * @return
     */
    Result queryById(Long id);

    /**
     * 更新
     * @param shop
     * @return
     */
    Result update(Shop shop);

    Result queryShopByType(Integer typeId, Integer current, Double x, Double y);
}
