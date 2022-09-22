package com.xh.flink.java.pojo;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * @Classname WebTherapySuperviseInfo
 * @Description 在线诊疗（咨询）监管信息
 * @Version 1.0.0
 * @Date 2022/9/20 15:19
 * @Created by xiaohuan
 */
@Data
@NoArgsConstructor
public class WebTherapySuperviseInfo {
    /**
     * 主键
     */
    private Long id;

    /**
     * 医疗机构编码参见互联网医院字典
     */
    private String orgCode;

    /**
     * 医疗机构名称
     */
    private String orgName;

    /**
     * 接诊时间形式如“yyyy-mm-dd”空格“hh:mm:ss”
     */
    private String visitTime;

    /**
     * 结束时间形式如“yyyy-mm-dd”空格“hh:mm:ss”
     */
    private String visitFinishTime;

    /**
     * 接诊科室编码
     */
    private String visitDeptCode;

    /**
     * 接诊科室名称
     */
    private String visitDeptName;

    /**
     * 接诊医师编码医生在互联网医院的编码
     */
    private String visitDocCode;

    /**
     * 接诊医师姓名
     */
    private String visitDocName;

    /**
     * 患者id互联网医院内部患者标识号
     */
    private String ptId;

    /**
     * 就诊流水号
     */
    private String medRdNo;

    /**
     * 是否复诊1：是，0：否
     */
    private Integer revisitFlag;

    /**
     * 初诊医院编码
     */
    private String firstVisitOrgCode;

    /**
     * 初诊医院名称
     */
    private String firstVisitOrgName;

    /**
     * 就诊类别编码参见就诊类别字典
     */
    private String medClassCode;

    /**
     * 就诊类别名称
     */
    private String medClassName;

    /**
     * price
     */
    private String price;

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
     * 患者出生日期形式如“yyyy-mm-dd”
     */
    private String ptBirthdateWait;

    /**
     * valid_cert_code
     */
    private String validCertCode;

    /**
     * valid_cert_name
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
     * 患者所在地区以国家统计局的《最新县及县以上行政区划代码》为准，填写地区代码
     */
    private String ptDistrict;

    /**
     * 联合参会医生列表列表的json格式字符串
     */
    private String consultDocList;

    /**
     * 疾病编码参见疾病字典，西医使用icd10，中医使用gb-95。在疾病的字段中，如果要传多个疾病，中间使用分隔符“|”分开
     */
    private String diseasesCode;

    /**
     * diseases_name
     */
    private String diseasesName;

    /**
     * 症状描述
     */
    private String complaintContent;

    /**
     * 现病史
     */
    private String presentIllness;

    /**
     * 既往史
     */
    private String pastHistory;

    /**
     * 咨询或就诊如果本次诊疗属于咨询型，则填“0”；如果本次诊疗属于就诊型，则填“1”
     */
    private String askOrMed;

    /**
     * 数据处理状态 0-hamis初始同步状态 1-已处理 2-已自动审核
     */
    private int handleStatus;

    /**
     * 最后更新时间 形式如 yyyy-mm-dd 空格 hh:mm:ss
     */
    private Timestamp updatedLast;

    /**
     * 患者出生日期形式如“yyyy-mm-dd”
     */
    private String ptBirthdate;
}
