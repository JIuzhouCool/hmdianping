package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.ScrollResult;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import jodd.util.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
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
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Autowired
    private UserServiceImpl userService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private IFollowService followService;
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
            queryBlogUser(blog);
            isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Integer id) {
        //首先进行Blog的查询
        Blog blog = getById(id);
        if (blog==null){
            return Result.fail("blog不存在");
        }
        //进行其它blog信息的封装
        queryBlogUser(blog);
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    /**
     * 判断blog是否被点赞了，更新blog的islike字段
     */
    private void isBlogLiked(Blog blog){
        //获取当前用户
        UserDTO user = UserHolder.getUser();
        if (user==null){
            //用户未登录
            return;
        }
        Long userId = user.getId();
        String key = RedisConstants.BLOG_LIKED_KEY+blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score!=null){
            blog.setIsLike(true);
        }else {
            blog.setIsLike(false);
        }
    }

    /**
     * 修改点赞功能
     * @param id
     * @return
     */
    @Override
    public Result likeBlog(Long id) {
        //首先获取用户
        Long userId = UserHolder.getUser().getId();
        //判断redis中的set集合是否已经点赞
        String key = RedisConstants.BLOG_LIKED_KEY+id ;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score!=null){
            //说明在集合里面,表示点过赞
            //数据库点赞数-1；
            boolean success = update().setSql("liked = liked-1").eq("id", id).update();
            //将用户从数据库中移除
            if (success){
                stringRedisTemplate.opsForZSet().remove(key,userId.toString());
            }else{
                return Result.fail("从Redis移除用户失败");
            }
        }else{
            //说明不在集合
            //进行点赞
            boolean success = update().setSql("liked = liked+1").eq("id", id).update();
            //添加进redis的set
            if (success){
                //说明添加成功
                stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
            }else{
                return Result.fail("添加进Redis的set中失败");
            }
        }
        return Result.ok();
    }

    /**
     * 查询博客的前五个喜欢列表
     * @param id
     * @return
     */
    @Override
    public Result queryBlogLikes(Long id) {
        String key  = RedisConstants.BLOG_LIKED_KEY+id;
        //首先从Redis的Zset中查询
        Set<String> stringIds = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (stringIds==null||stringIds.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = stringIds.stream().map(Long::valueOf).toList();
        String string_ids = StringUtil.join(ids, ",");
        //根据ids查询用户
        //不能直接使用userService.listByIds(),里面使用的是in(),会根据id自己排序，不会根据自己给定的顺序
        List<User> users = userService.query().in("id",ids).last("ORDER BY FIELD(id,"+string_ids+")").list();
        //将users封装为userDTO进行返回
        List<UserDTO> userDTOS = users.stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).toList();
        return Result.ok(userDTOS);
    }

    /**
     * 使用feed流推送给粉丝
     * @param blog
     * @return
     */
    @Override
    public Result saveBlog(Blog blog) {
        //获取当前用户
        UserDTO user = UserHolder.getUser();
        //设置用户id
        blog.setUserId(user.getId());
        //保存blog
        boolean isSave = save(blog);
        //看是否保存成功
        if (isSave){
            //从数据库中查询粉丝
            List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
            //推送给粉丝
            for (Follow follow : follows) {
                Long userId = follow.getUserId();
                //推送
                String key = RedisConstants.FEED_KEY+userId;
                stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
            }
        }
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        //首先获取当前用户
        UserDTO user = UserHolder.getUser();
        //获取id
        if (user==null){
            return Result.fail("当前用户为null");
        }
        Long userId = user.getId();
        String key = RedisConstants.FEED_KEY+userId;
        //根据key去zset中查询
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate
                .opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        //判断
        if (typedTuples==null||typedTuples.isEmpty()){
            return Result.fail("feed流数据为空");
        }
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int ofs = 1;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            //获取id
            String idStr = typedTuple.getValue();
            ids.add(Long.valueOf(idStr));
            long time = typedTuple.getScore().longValue();
            if (time==minTime){
                ofs++;
            }else{
                minTime = time;//遍历完集合正好是最小的时间
                ofs = 1;
            }
        }
        String string_ids = StringUtil.join(ids, ",");
        List<Blog> blogs = query().in("id",ids).last("ORDER BY FIELD(id,"+string_ids+")").list();
        for (Blog blog : blogs) {
            queryBlogUser(blog);
            isBlogLiked(blog);
        }
        ScrollResult scrollResult = new ScrollResult(blogs, minTime, ofs);
        return Result.ok(scrollResult);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
