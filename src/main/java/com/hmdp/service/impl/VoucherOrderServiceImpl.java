package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private RedisIdWorker redisIdWorker;

    @Override
    public Result seckillVoucher(Long voucherId) {
        //首先查询数据库
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //获取时间
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀未开始");
        }
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束");
        }
        //判断库存
        if (voucher.getStock() < 1) {
            return Result.fail("库存已经没有了");
        }
        //首先添加锁，给UserID加锁，确保用户一人一单，锁在方法外面是确保事务的完整性
        Long userId = UserHolder.getUser().getId();
        synchronized (userId.toString().intern()) {
            //由于加锁是给this加锁，可能导致事务的锁失效，所以获取代理对象，使用代理对象
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }
    }

    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        //查询订单
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            //用户已经购买过了
            return Result.fail("用户已经买了一次");
        }
        //扣减库存
        boolean success = seckillVoucherService
                .update()
                .setSql("stock = stock-1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!success) {
            return Result.fail("库存不足");
        }
        VoucherOrder voucherOrder = new VoucherOrder();
        //创建订单
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //取出用户
        UserDTO user = UserHolder.getUser();
        voucherOrder.setUserId(user.getId());
        voucherOrder.setVoucherId(voucherId);
        //写入数据库
        save(voucherOrder);
        return Result.ok(orderId);
    }
}
