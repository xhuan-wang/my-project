package com.xh.flink.java.pojo;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * @Classname DoctorBasicInfo
 * @Description TODO
 * @Version 1.0.0
 * @Date 2022/9/21 16:39
 * @Created by xiaohuan
 */
@Data
@NoArgsConstructor
public class DoctorBasicInfo {

    /**
     * 主键
     */
//    private String id;

    /**
     * 签约医疗机构编码参见互联网医院字典
     */
    private String orgCode;

    /**
     * 签约医疗机构名称
     */
    private String orgName;

    /**
     * 机构内人员编码
     */
    private String inDocCode;

    /**
     * 人员类型审方药师、诊疗医师
     */
    private String docType;

    /**
     * 姓名
     */
    private String docName;

    /**
     * 性别编码
     */
    private String geCode;

    /**
     * 性别名称
     */
    private String geName;

    /**
     * 民族编码
     */
    private String nationCode;

    /**
     * 民族名称
     */
    private String nationName;

    /**
     * 通信地址
     */
    private String docAddress;

    /**
     * 学历大学、硕士、博士
     */
    private String docEdu;

    /**
     * 行政职务
     */
    private String docPosition;

    /**
     * doc_comment
     */
    private String docComment;

    /**
     * 职称编码
     */
    private String titleCode;

    /**
     * 职称名称 参见职称字典
     */
    private String titleName;

    /**
     * 医师第一执业机构编码组织机构代码
     */
    private String workInstCode;

    /**
     * 医师第一执业机构名称组织机构名称
     */
    private String workInstName;

    /**
     * 编制科室编码
     */
    private String docDeptCode;

    /**
     * 编制科室名称
     */
    private String docDeptName;

    /**
     * 联系手机号
     */
    private String docTel;

    /**
     * 身份证号
     */
    private String idCard;

    /**
     * 执业证号
     */
    private String pracNo;

    /**
     * 执业证取得时间形式如“yyyy-mm-dd”
     */
    private String pracRecDate;

    /**
     * 资格证号
     */
    private String certNo;

    /**
     * 资格证取得时间形式如“yyyy-mm-dd”
     */
    private String certRecDate;

    /**
     * 职称证号
     */
    private String titleNo;

    /**
     * 职称证取得时间形式如“yyyy-mm-dd”
     */
    private String titleRecDate;

    /**
     * 医师执业类别对应执业证的执业类别
     */
    private String pracType;

    /**
     * 医师执业范围对应执业证的执业范围
     */
    private String pracScopeApproval;

    /**
     * 最近连续两个周期的医师定期考核合格是否合格连续两个周期的医师定期考核合格是否合格
     */
    private String qualifyOrNot;

    /**
     * professional
     */
    private String professional;

    /**
     * 是否同意以上条款医师是否同意多点执业备案信息表上的条款
     */
    private String agreeTerms;

    /**
     * 医师执业起始时间医师在互联网医院内的执业起始时间，形式如yyyy-mm-dd
     */
    private String docMultiSitedDateStart;

    /**
     * 医师执业终止时间医师在互联网医院内的执业终止时间，形式如yyyy-mm-dd
     */
    private String docMultiSitedDateEnd;

    /**
     * 申请拟执业医疗机构意见对应备案信息表的医院意见
     */
    private String hosOpinion;

    /**
     * 申请拟执业医疗机构意见时间对应备案信息表的医院意见填写时间，形式如yyyy-mm-dd
     */
    private String hosOpinionDate;

    /**
     * 海南是否已备案如果海南已备案，则值为“1”，只有已备案的医师才可以问诊；否则值为0
     */
    private Integer recordFlag;

    /**
     * 医院是否已确认互联网医院需要确认医师信息的正确性，已确认则值为1，否则值为0
     */
    private Integer hosConfirmFlag;

    /**
     * 海南处方开具权是否备案海南处方开具权是否备案标志，已备案则值为1，否则值为0
     */
    private Integer presRecordFlag;

    /**
     * 医生生日
     */
    private String docBirthdate;

    /**
     * 医生住址邮政编码
     */
    private String docPostalCode;

    /**
     * 医生健康状况
     */
    private String healthCondition;

    /**
     * 业务水平考核机构或组织名称、考核培训时间及结果
     */
    private String appraisalContent;

    /**
     * 何时何地因何种原因受过何种处罚或处分
     */
    private String punishContent;

    /**
     * 其它要说明的问题
     */
    private String otherContent;

    /**
     * 医师执业级别:执业医师、执业助理医、执业药师
     */
    private String pracLevel;

    /**
     * 上传《医疗机构执业许可证》上的登记号
     */
    private String orgRegNo;

    /**
     * 上传《医疗机构执业许可证》上的地址
     */
    private String orgAddress;

    /**
     * 上传《医疗机构执业许可证》上的邮政编码
     */
    private String orgPostalCode;

    /**
     * 单位电话
     */
    private String orgTel;

    /**
     * 数据处理状态 0-hamis初始同步状态 1-已处理 2-已自动审核
     */
    private String handleStatus;

    /**
     * 最后更新时间 形式如 yyyy-mm-dd 空格 hh:mm:ss
     */
    private Timestamp updatedLast;

    /**
     * 更新者
     */
    private Long updater;

    /**
     * 更新时间
     */
    private String updateDate;
}
