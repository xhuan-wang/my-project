package com.xh.flink.java.pojo;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * @Classname Prescription
 * @Description 处方详情
 * @Version 1.0.0
 * @Date 2022/9/21 16:30
 * @Created by xiaohuan
 */
@Data
@NoArgsConstructor
public class Prescription {

    /**
     * id
     */
    private Long id;

    /**
     * 处方号 互联网医院内部处方号
     */
    private String presNo;

    /**
     * 处方类别编码
     */
    private String presClassCode;

    /**
     * 处方类别名称
     */
    private String presClassName;

    /**
     * 患者id互联网医院内部患者标识号
     */
    private String ptId;

    /**
     * 患者姓名
     */
    private String ptNo;

    /**
     * 性别编码
     */
    private String geCode;

    /**
     * 性别名称
     */
    private String geName;

    /**
     * 患者年龄
     */
    private Integer ptAge;

    /**
     * 出生日期形式如 yyyy-mm-dd
     */
    private String birthday;

    /**
     * 有效证件类型编码
     */
    private String validCertCode;

    /**
     * 有效证件类型名称
     */
    private String validCertName;

    /**
     * 身份证号
     */
    private String idNo;

    /**
     * 患者手机号
     */
    private String ptTel;

    /**
     * 患者所在地区
     */
    private String ptDistrict;

    /**
     * 保险类别编码
     */
    private String insClassCode;

    /**
     * 保险类别名称
     */
    private String insClassName;

    /**
     * 就诊流水号
     */
    private String medRdNo;

    /**
     * 就诊类别编码参见就诊类别字典
     */
    private String medClassCode;

    /**
     * 就诊类别名称
     */
    private String medClassName;

    /**
     * 医疗机构编码 参见互联网医院字典
     */
    private String orgCode;

    /**
     * 医疗机构名称
     */
    private String orgName;

    /**
     * 就诊科室编码参见科室字典
     */
    private String visitDeptCode;

    /**
     * 就诊科室名称
     */
    private String visitDeptName;

    /**
     * 开方科室编码参见科室字典
     */
    private String presDeptCode;

    /**
     * 开方科室名称
     */
    private String presDeptName;

    /**
     * 开方时间
     */
    private String presTime;

    /**
     * 开方医生编码医生在互联网医院的内部编码
     */
    private String presDocCode;

    /**
     * 开方医生姓名
     */
    private String presDocName;

    /**
     * 开方医师照片数据 base64编码，图片大小限制不超过200k，图片文件格式是jpg
     */
    private String presDocPhoteData;

    /**
     * 审核时间
     */
    private String reviewTime;

    /**
     * 审核医生编码
     */
    private String reviewDocCode;

    /**
     * 审核医生姓名
     */
    private String reviewDocName;

    /**
     * 审方时间
     */
    private String trialTime;

    /**
     * 审方药师编码
     */
    private String trialDocCode;

    /**
     * 审方药师姓名
     */
    private String trialDocName;

    /**
     * 诊断编码类型表示疾病编码使用的标准
     */
    private String diagCodeType;

    /**
     * 疾病编码参见疾病字典，西医使用icd10，中医使用gb-95
     */
    private String diseasesCode;

    /**
     * diseases_name
     */
    private String diseasesName;

    /**
     * 疾病分类用来判断药量是否超标
     */
    private Integer diseasesType;

    /**
     * 行动不便标志用来判断药量是否超标
     */
    private Integer mobilityFlag;

    /**
     * 病情稳定需长期服药标志用来判断药量是否超标
     */
    private Integer longMedicalFlag;

    /**
     * 处方有效期（单位天）
     */
    private Integer presEffecDays;

    /**
     * total_price
     */
    private String totalPrice;

    /**
     * 数据处理状态 0-hamis初始同步状态 1-已处理 2-已自动审核
     */
    private int handleStatus;

    /**
     * 最后更新时间
     */
    private Timestamp updatedLast;
}
