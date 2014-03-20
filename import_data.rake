#encoding: utf-8

=begin
 离休管理系统数据库导入规则说明
 对象：
   待导入表、retirees及字典序表
 规则：
   1.retirees表中有但是待导入表中没有的字段设为NULL
   2.处级单位字段信息导入companies表,暂不考虑管理单位表 
   3.tutor是“是否博导”，若没有数据先不管 
   4.技术级别对应technics表，为数据字典，对应列为technic_id，技术级类对应technic_title字段，string类型。
   5.行政级别executive_title字段，string类型,直接写入
   6.所在支部，现待遇忽略
   7.民族字段在我们的表中加一列
   8.老有所为相关内容对应somethings表，可稍后再弄
   9.是否空巢家，身体状况，其它联络方式，是否外地，备注等信息先忽略
   10.retired_type_id是离退休类型，离休or退休。对应数据字典表retired_types
   11.treatment_level_id忽略
=end

namespace :db do
  desc "import data via task" 

  task(:import_data => :environment) do 
    # 获得所有处级单位，存到companies数组
    # 获得技术级别, 存到technics数组 
    # retired_type共两种类型，离休为0，退休为1, 存于数组retired_type
    # 获得文化程度, 存到educations数组
    # 获得政治面貌，存到politicals
    companies = []
    technics = []
    educations = []
    politicals = []
    retired_types = ["离休", "退休"] 
    File.open("./dbf-utf8.csv", "r") do |inline|
      flag = false
      while(line = inline.gets) do
          if (flag)
            info = line.split(',')
            companies << info[1]
            technics << info[9]
            educations << info[16]
            politicals << info[17]
          end
          flag = true
      end
      # 删除重复
      companies.uniq!
      technics.uniq!
      educations.uniq!
      politicals.uniq!
  
      # 把数据写进数据库
      companies.each do |company|
        Company.create(title: company)
      end

      technics.each do |technic|
        Technic.create(title: technic)
      end

      educations.each do |education|
        Education.create(title: education)
      end

      politicals.each do |political|
        Political.create(title: political)
      end
    
      retired_types.each do |retired_type|
        RetiredType.create(title: retired_type)
      end
    
    end

    # 第二次遍历录入retirees数据表
    File.open("./dbf-utf8.csv", "r") do |inline|
      flag = false
      while(line = inline.gets) do
          if (flag)
            info = line.split(',')
            # 生成一条记录
            retiree = {}
            retiree['name'] = info[5]
            retiree['gender'] = info[14]
            retiree['birthday'] = info[7]
            retiree['join_part'] = info[18]
            retiree['start_work'] = info[8]
            retiree['retired_date'] = info[6]
            retiree['address'] = info[19]
            retiree['tel'] = info[20]
            retiree['nation'] = info[15]
            retiree['executive_title'] = info[11]
            retiree['technic_title'] = info[10]
            # 退休or离休
            retiree['retired_type_id'] = ((info[13] == "离休") ? 0 : 1)
            # 处级单位
            retiree['company_id'] = companies.index(info[1]) + 1
            # 政治面貌
            retiree['political_id'] = politicals.index(info[17]) + 1
            # 文化程度
            retiree['education_id'] = educations.index(info[16]) + 1
            # 技术级别
            retiree['technic_id'] = technics.index(info[9]) + 1
            
            Retiree.create(retiree) 
          end
          flag = true
      end
    end
  end
end

