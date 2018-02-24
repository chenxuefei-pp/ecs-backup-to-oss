# ecs-backup-to-oss

## 备份ECS重要资料到OSS归档存储，减少全盘备份费用

Useage:

    # 首先在文件中配置好access_token
    # 备份成功后会自动删除<max_backup_day>之前的备份
    python oss_backuper.py <backup_prefix> <backup_dir> <max_backup_day>

    