package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.首先校验手机号，调用封装好的工具类
        if (RegexUtils.isPhoneInvalid(phone)) {
            //不符合 返回错误信息
            return Result.fail("手机号格式错误");
        }
        //符合 生成验证码 也是调用工具类
        String code = RandomUtil.randomNumbers(6);
//        //保存验证码到session
//        session.setAttribute("code",code);
        //解决多个tomcat共享问题，使用redis进行v存储 //设置有效期
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY+ phone,code,RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //发送验证码
        log.info("假设发送验证码:{}",code);
        //返回ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //首先校验手机号和验证码
        //1.校验手机号
        if (RegexUtils.isPhoneInvalid(loginForm.getPhone())) {
            return Result.fail("手机号格式错误");
        }
//        //从session中取出验证码
//        Object cacheCode = session.getAttribute("code");
        //升级，从redis里面进行验证码的获取
        String cacheCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + loginForm.getPhone());
        //从表单里面提取出code
        String code = loginForm.getCode();
        //校验cacheCode
        if (cacheCode==null || !cacheCode.toString().equals(code)){
            //如果不一致或者为空
            return Result.fail("验证码错误");
        }
        //根据mp提供的基础方法进行查询
        User user = query().eq("phone", loginForm.getPhone()).one();
        if(user == null){
            //不存在，创建用户
            user = createUserWithPhone(loginForm.getPhone());
        }
//        //保存用户信息到session中
//        session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
        //保存用户到Redis中
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        //将userDTO转为map进行存储
        Map<String, Object> map = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions
                        .create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName,fieldValue)->fieldValue.toString()));
        //构造token
        String token = UUID.randomUUID().toString(true);
        stringRedisTemplate.opsForHash().putAll(RedisConstants.LOGIN_USER_KEY+token,map);
        //设置有效期半小时
        stringRedisTemplate.expire(RedisConstants.LOGIN_USER_KEY+token,RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);
        return Result.ok(token);
    }

    /**
     * 实现签到
     * @return
     */
    @Override
    public Result sign() {
        //首先获取用户
        Long userId = UserHolder.getUser().getId();
        //获取日期
        LocalDateTime now = LocalDateTime.now();
        //将日期标准化
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyy/MM"));
        String key = RedisConstants.USER_SIGN_KEY+userId+keySuffix;
        //获取当天的天数
        int dayOfMonth = now.getDayOfMonth();
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth-1,true);
        return Result.ok();
    }

    /**
     * 连续签到天数计算
     * @return
     */
    @Override
    public Result signCount() {
        //首先获取当前用户id
        Long userId = UserHolder.getUser().getId();
        LocalDateTime now = LocalDateTime.now();
        //获取当前天数
        int dayOfMonth = now.getDayOfMonth();
        String keySufix = now.format(DateTimeFormatter.ofPattern(":yyyy/MM"));
        String key = RedisConstants.USER_SIGN_KEY+userId+keySufix;
        //使用redis的bitFeild查询
        List<Long> res = stringRedisTemplate
                .opsForValue()
                .bitField(key,
                        BitFieldSubCommands.create().get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));
        //首先判断res的长度
        if (res==null||res.isEmpty()){
            return Result.ok(0);
        }
        //获取查询到的数据
        Long days = res.get(0);
        //需要进行判断
        if (days==null||days == 0){
            return Result.ok(0);
        }
        int count = 0;
        //进行统计
        while (true){
            if ((days&1)==1){
                //说明签到了
                count++;
            }else {
                //没有签到
                break;
            }
            //右移一位
            days >>>=1;
        }
        return Result.ok(count);
    }

    private User createUserWithPhone(String phone) {
        //创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX +RandomUtil.randomString(10));
        //保存用户信息
        save(user);
        return user;
    }
}
