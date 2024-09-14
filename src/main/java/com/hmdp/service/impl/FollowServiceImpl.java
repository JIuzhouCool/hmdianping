package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private IUserService userService;
    @Override
    @Transactional
    public Result follow(Long followUserId, Boolean isFollow) {
        //首先获取登录的用户
        UserDTO user = UserHolder.getUser();
        if(user==null){
            return Result.fail("用户暂时未登录");
        }
        //判断是关注还是取关
        String key = "follows:"+user.getId();//Redis的key
        if(isFollow){
            //关注，新增数据
            Follow follow = new Follow();
            follow.setUserId(user.getId());
            follow.setFollowUserId(followUserId);
            boolean isSave = save(follow);
            if(isSave){
                //把关注的用户id放入到Redis的集合
                stringRedisTemplate.opsForSet().add(key,followUserId.toString());
            }
        }else{
            //取关，删除数据
            boolean isRemove = remove(new QueryWrapper<Follow>().eq("user_id", user.getId()).eq("follow_user_id", followUserId));
            if (isRemove){
                //把关注的用户id从Redis中移除
                stringRedisTemplate.opsForSet().remove(key,followUserId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        Integer count = query().eq("user_id", UserHolder.getUser().getId()).eq("follow_user_id", followUserId).count();
        //判断
        return Result.ok(count>0);
    }

    @Override
    public Result followCommons(Long id) {
        //首先获取当前用户
        UserDTO user = UserHolder.getUser();
        if (user==null){
            return Result.fail("用户不存在");
        }
        Long userId = user.getId();
        //根据当前id和userId到Redis中查询
        String key1 = "follows:"+id;
        String key2 = "follows:"+userId;
        //根据Redis求出交集
        Set<String> res = stringRedisTemplate.opsForSet().intersect(key1, key2);
        //判断交集是否为空
        if (res==null||res.isEmpty()){
            return Result.fail("共同关注交集为空");
        }
        List<Long> ids = res.stream().map(Long::valueOf).toList();
        List<UserDTO> userDTOS = userService.listByIds(ids).stream().map(user1 -> BeanUtil.copyProperties(user, UserDTO.class)).toList();
        return Result.ok(userDTOS);
    }
}
