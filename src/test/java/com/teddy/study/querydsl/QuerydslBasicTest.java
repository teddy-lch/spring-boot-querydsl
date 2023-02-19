package com.teddy.study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import com.teddy.study.querydsl.dto.MemberDto;
import com.teddy.study.querydsl.dto.QMemberDto;
import com.teddy.study.querydsl.entity.Member;
import com.teddy.study.querydsl.entity.QMember;
import com.teddy.study.querydsl.entity.QTeam;
import com.teddy.study.querydsl.entity.Team;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;

import java.util.List;

import static com.querydsl.jpa.JPAExpressions.*;
import static com.teddy.study.querydsl.entity.QMember.member;
import static com.teddy.study.querydsl.entity.QTeam.*;
import static org.assertj.core.api.Assertions.*;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory queryFactory;

    @BeforeEach
    public void before() {
        queryFactory = new JPAQueryFactory(em);

        Team teamA = new Team("TeamA");
        Team teamB = new Team("TeamB");

        em.persist(teamA);
        em.persist(teamB);

        Member memberA = new Member("member1", 10, teamA);
        Member memberB = new Member("member2", 20, teamA);
        Member memberC = new Member("member3", 30, teamB);
        Member memberD = new Member("member4", 40, teamB);

        em.persist(memberA);
        em.persist(memberB);
        em.persist(memberC);
        em.persist(memberD);
    }

    @Test
    public void startJPQL() {
        // member1 찾기
        String qlString =
                "select m from Member m " +
                "where m.username = :username";

        Member findMember = em.createQuery(qlString, Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl() {
//        JPAQueryFactory queryFactory = new JPAQueryFactory(em); // 필드변수로 뺴서 사용해도됨
//        QMember m = new QMember("m");
//        QMember m = QMember.member;

        Member findMember = queryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() {
        Member member1 = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10)))
                .fetchOne();

        assertThat(member1.getUsername()).isEqualTo("member1");
    }

    @Test
    public void searchAndParam() {
        Member member1 = queryFactory
                .selectFrom(member)
                .where(
                        member.username.eq("member1"),
                        member.age.eq(10)
                )
                .fetchOne();

        assertThat(member1.getUsername()).isEqualTo("member1");
    }

    @Test
    public void resultFetch() {
//        List<Member> fetch = queryFactory
//                .selectFrom(member)
//                .fetch();
//
//        Member member1 = queryFactory
//                .selectFrom(member)
//                .fetchOne();
//
//        Member member2 = queryFactory
//                .selectFrom(member)
//                .fetchFirst();

        QueryResults<Member> memberQueryResults = queryFactory
                .selectFrom(member)
                .fetchResults();
        memberQueryResults.getTotal();
        List<Member> results = memberQueryResults.getResults();

        long total = queryFactory
                .selectFrom(member)
                .fetchCount();
    }

    @Test
    public void sort() {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);

        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();
    }

    @Test
    public void paging1() {
        QueryResults<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(0)
                .limit(2)
                .fetchResults();

        assertThat(result.getTotal()).isEqualTo(4);
        assertThat(result.getLimit()).isEqualTo(2);
        assertThat(result.getOffset()).isEqualTo(1);
    }

    @Test
    public void aggregation() {
        List<Tuple> result = queryFactory
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max()
                )
                .from(member)
                .fetch();

        Tuple tuple = result.get(0);
        Long count = tuple.get(member.count());

    }

    @Test
    public void group() throws Exception {
        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("TeamA");
        assertThat(teamB.get(team.name)).isEqualTo("TeamB");
    }

    @Test
    public void join() throws Exception {
        // given
        List<Member> result = queryFactory
                .select(
                        member
                )
                .from(member)
                .join(member.team, team)
                .where(team.name.eq("TeamA"))
                .fetch();

        // When

        // Then
        assertThat(result).extracting("username").containsExactly("member1", "member2");
    }

    @Test
    public void theta_join() throws Exception {
        // given
        em.persist(new Member("TeamA"));
        em.persist(new Member("TeamB"));

        // When
        List<Member> result = queryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        // Then
        assertThat(result)
                .extracting("username")
                .containsExactly("TeamA", "TeamB");
    }

    @Test
    public void join_on_filtering() throws Exception {
        // given
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team).on(team.name.eq("TeamA"))
                .fetch();

        // When

        // Then
    }

    @Test
    public void join_on_no_relation() throws Exception {
        // given
        em.persist(new Member("TeamA"));
        em.persist(new Member("TeamB"));

        // When
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))
                .where(member.username.eq(team.name))
                .fetch();

        // Then
        assertThat(result)
                .extracting("username")
                .containsExactly("TeamA", "TeamB");
    }

    @PersistenceUnit
    EntityManagerFactory emf;
    @Test
    public void fetchJoinNo() throws Exception {
        // given
        em.flush();
        em.clear();

        Member member1 = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(member1.getTeam());

        assertThat(loaded).as("페치 조인 미적용").isFalse();
        // When

        // Then
    }

    @Test
    public void fetchJoinYes() throws Exception {
        // given
        em.flush();
        em.clear();

        Member member1 = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(member1.getTeam());

        assertThat(loaded).as("페치 조인 미적용").isTrue();
        // When

        // Then
    }

    @Test
    public void subQuery() throws Exception {
        // given
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age").containsExactly(40);
        // When

        // Then
    }

    @Test
    public void selectSubQuery() throws Exception {
        // given
        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = queryFactory
                .select(member.username,
                        select(memberSub.age.avg())
                                .from(memberSub)
                )
                .from(member)
                .fetch();

        // When

        // Then

    }

    @Test
    public void basicCase() throws Exception {
        // given

        List<String> result = queryFactory
                .select(
                        member.age
                                .when(10).then("열살")
                                .when(20).then("스무살")
                                .otherwise("기타")
                )
                .from(member)
                .fetch();


        for (String s : result) {
            System.out.println("s = " + s);
        }
        // When

        // Then

    }

    @Test
    public void complexCase() throws Exception {
        // given

        List<String> result = queryFactory
                .select(new CaseBuilder().when(member.age.between(0, 20)).then("0~20살").otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
        // When

        // Then

    }

    @Test
    public void constant() throws Exception {
        // given
        List<Tuple> result = queryFactory
                .select(member.username, Expressions.constant("A"), member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .fetch();

        // When
        for (Tuple s : result) {
            System.out.println("s = " + s);
        }
        // Then

    }

    @Test
    public void findDtoBySetter() throws Exception {
        // given
        List<MemberDto> result = queryFactory
                .select(Projections.bean(MemberDto.class, // dto의 setter 메소드로 주입
                        member.username,
                        member.age
                ))
                .from(member)
                .fetch();

        // When
        result.stream().forEach(r -> System.out.println("result : " + r));

        // Then

    }
    @Test
    public void findDtoByField() throws Exception {
        // given
        List<MemberDto> result = queryFactory
                .select(Projections.fields(MemberDto.class, // dto의 field 에 주입
                        member.username,
                        member.age
                ))
                .from(member)
                .fetch();

        // When
        result.stream().forEach(r -> System.out.println("result : " + r));

        // Then

    }

    @Test
    public void findDtoByConstructor() throws Exception {
        // given
        List<MemberDto> result = queryFactory
                .select(Projections.constructor(MemberDto.class, // dto의 생성자 에 주입 (순서가 맞아야함)
                        member.username,
                        member.age
                ))
                .from(member)
                .fetch();

        // When
        result.stream().forEach(r -> System.out.println("result : " + r));

        // Then

    }

    @Test
    public void findBtoByQueryProjection() throws Exception {
        // given

        List<MemberDto> result = queryFactory
                .select(new QMemberDto(
                        member.username,
                        member.age
                ))
                .from(member)
                .fetch();

        result.stream().forEach(r -> System.out.println("result : " + r));

        // When

        // Then

    }

    @Test
    public void dynamic_query_booleanbuilder() throws Exception {
        // given
        String username = "member1";
        Integer ageParam = 10;

        List<Member> result = serachMemeber1(username, ageParam);

        // When

        // Then

    }

    private List<Member> serachMemeber1(String username, Integer ageParam) {
        BooleanBuilder builder = new BooleanBuilder();
        if (username != null) {
            builder.and(member.username.eq(username));
        }
        if (ageParam != null) {
            builder.and(member.age.eq(ageParam));
        }

        return queryFactory
                .selectFrom(member)
                .where(builder)
                .fetch();
    }

    @Test
    public void dynamic_query_where() throws Exception {
        // given
        String username = "member1";
        Integer ageParam = 10;

        List<Member> result = serachMemeber2(username, ageParam);

        // When

        // Then

    }

    private List<Member> serachMemeber2(String username, Integer ageParam) {
        return queryFactory
                .selectFrom(member)
//                .where(usernameEq(username), ageEq(ageParam))
                .where(allEq(username, ageParam))
                .fetch();
    }

    private BooleanExpression usernameEq(String username) {
        if (username == null) {
            return  null;
        }
        return member.username.eq(username);
    }

    private BooleanExpression ageEq(Integer ageParam) {
        if (ageParam == null) {
            return null;
        }
        return member.age.eq(ageParam);
    }

    private BooleanExpression allEq(String username, Integer age) {
        return usernameEq(username).and(ageEq(age));
    }

    @Test
    public void bulkUpdate() throws Exception {
        // given

        long count = queryFactory
                .update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28))
                .execute();

        // When
        // 벌크 처리시 DB에 바로 적용되어 영속성 컨텍스트 랑 다름 -> 해결하려면 em.flush() em.clear() 로 클리어 후 진행해야함.
        em.flush();
        em.clear();

        List<Member> result = queryFactory
                .selectFrom(member)
                .fetch();

        for (Member member : result) {
            System.out.println("member : " + member);
        }
        // Then

    }

    @Test
    public void bulkAdd() throws Exception {
        // given
        queryFactory
                .update(member)
//                .set(member.age, member.age.add(1))
                .set(member.age, member.age.multiply(2))
                .execute();

        // When

        // Then

    }

    @Test
    public void bulkDelete() throws Exception {
        // given

        queryFactory
                .delete(member)
                .where(member.age.gt(18))
                .execute();

        // When

        // Then

    }

    @Test
    public void sqlFunction() throws Exception {
        // given

        List<String> result = queryFactory
                .select(Expressions.stringTemplate(
                        "function('replace', {0}, {1}, {2})"
                        , member.username, "member", "M"))
                .from(member)
                .fetch();

        // When
        for (String member : result) {
            System.out.println("member : " + member);
        }

        // Then

    }

    @Test
    public void sqlFunction2() throws Exception {
        // given

        List<String> result = queryFactory
                .select(member.username)
                .from(member)
                .where(
//                        member.username.eq(Expressions.stringTemplate("function('lower', {0})", member.username)))
                        member.username.eq(member.username.lower()))
                .fetch();

        for (String member : result) {
            System.out.println("member : " + member);
        }

        // When

        // Then

    }

}
