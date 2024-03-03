package kcl

import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.regions.Region;
import com.amazonaws.ResponseMetadata
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.cloudwatch.model.*
import com.amazonaws.services.cloudwatch.waiters.AmazonCloudWatchWaiters


class NoopKclCloudwatch : AmazonCloudWatch {
    @Deprecated("")
    override fun setEndpoint(s: String?) {
    }

    @Deprecated("")
    override fun setRegion(region: Region?) {
    }


    override fun deleteAlarms(deleteAlarmsRequest: DeleteAlarmsRequest?): DeleteAlarmsResult? {
        return null
    }

    override fun deleteAnomalyDetector(deleteAnomalyDetectorRequest: DeleteAnomalyDetectorRequest?): DeleteAnomalyDetectorResult? {
        return null
    }

    override fun describeAlarmHistory(describeAlarmHistoryRequest: DescribeAlarmHistoryRequest?): DescribeAlarmHistoryResult? {
        return null
    }

    override fun describeAlarmHistory(): DescribeAlarmHistoryResult? {
        return null
    }

    override fun describeAlarms(describeAlarmsRequest: DescribeAlarmsRequest?): DescribeAlarmsResult? {
        return null
    }

    override fun describeAlarms(): DescribeAlarmsResult? {
        return null
    }

    override fun describeAlarmsForMetric(describeAlarmsForMetricRequest: DescribeAlarmsForMetricRequest?): DescribeAlarmsForMetricResult? {
        return null
    }

    override fun describeAnomalyDetectors(describeAnomalyDetectorsRequest: DescribeAnomalyDetectorsRequest?): DescribeAnomalyDetectorsResult? {
        return null
    }

    override fun describeInsightRules(describeInsightRulesRequest: DescribeInsightRulesRequest?): DescribeInsightRulesResult? {
        return null
    }

    override fun disableAlarmActions(disableAlarmActionsRequest: DisableAlarmActionsRequest?): DisableAlarmActionsResult? {
        return null
    }

    override fun disableInsightRules(disableInsightRulesRequest: DisableInsightRulesRequest?): DisableInsightRulesResult? {
        return null
    }

    override fun enableAlarmActions(enableAlarmActionsRequest: EnableAlarmActionsRequest?): EnableAlarmActionsResult? {
        return null
    }

    override fun enableInsightRules(enableInsightRulesRequest: EnableInsightRulesRequest?): EnableInsightRulesResult? {
        return null
    }

    override fun getMetricStatistics(getMetricStatisticsRequest: GetMetricStatisticsRequest?): GetMetricStatisticsResult? {
        return null
    }

    override fun getMetricStream(getMetricStreamRequest: GetMetricStreamRequest?): GetMetricStreamResult? {
        return null
    }

    override fun getMetricWidgetImage(getMetricWidgetImageRequest: GetMetricWidgetImageRequest?): GetMetricWidgetImageResult? {
        return null
    }

    override fun listMetrics(listMetricsRequest: ListMetricsRequest?): ListMetricsResult? {
        return null
    }

    override fun listMetrics(): ListMetricsResult? {
        return null
    }

    override fun listTagsForResource(listTagsForResourceRequest: ListTagsForResourceRequest?): ListTagsForResourceResult? {
        return null
    }

    override fun putAnomalyDetector(putAnomalyDetectorRequest: PutAnomalyDetectorRequest?): PutAnomalyDetectorResult? {
        return null
    }

    override fun putCompositeAlarm(putCompositeAlarmRequest: PutCompositeAlarmRequest?): PutCompositeAlarmResult? {
        return null
    }

    override fun putMetricAlarm(putMetricAlarmRequest: PutMetricAlarmRequest?): PutMetricAlarmResult? {
        return null
    }

    override fun putMetricData(putMetricDataRequest: PutMetricDataRequest?): PutMetricDataResult? {
        return null
    }

    override fun putMetricStream(putMetricStreamRequest: PutMetricStreamRequest?): PutMetricStreamResult? {
        return null
    }

    override fun setAlarmState(setAlarmStateRequest: SetAlarmStateRequest?): SetAlarmStateResult? {
        return null
    }

    override fun startMetricStreams(startMetricStreamsRequest: StartMetricStreamsRequest?): StartMetricStreamsResult? {
        return null
    }

    override fun stopMetricStreams(stopMetricStreamsRequest: StopMetricStreamsRequest?): StopMetricStreamsResult? {
        return null
    }

    override fun tagResource(tagResourceRequest: TagResourceRequest?): TagResourceResult? {
        return null
    }

    override fun untagResource(untagResourceRequest: UntagResourceRequest?): UntagResourceResult? {
        return null
    }

    override fun shutdown() {}

    override fun getCachedResponseMetadata(amazonWebServiceRequest: AmazonWebServiceRequest?): ResponseMetadata? {
        return null
    }

    override fun waiters(): AmazonCloudWatchWaiters? {
        return null
    }

    override fun deleteDashboards(deleteDashboardsRequest: DeleteDashboardsRequest?): DeleteDashboardsResult? {
        return null
    }

    override fun deleteInsightRules(deleteInsightRulesRequest: DeleteInsightRulesRequest?): DeleteInsightRulesResult? {
        return null
    }

    override fun deleteMetricStream(deleteMetricStreamRequest: DeleteMetricStreamRequest?): DeleteMetricStreamResult? {
        return null
    }

    override fun getDashboard(getDashboardRequest: GetDashboardRequest?): GetDashboardResult? {
        return null
    }

    override fun getInsightRuleReport(getInsightRuleReportRequest: GetInsightRuleReportRequest?): GetInsightRuleReportResult? {
        return null
    }

    override fun getMetricData(getMetricDataRequest: GetMetricDataRequest?): GetMetricDataResult? {
        return null
    }

    override fun listDashboards(listDashboardsRequest: ListDashboardsRequest?): ListDashboardsResult? {
        return null
    }

    override fun listManagedInsightRules(listManagedInsightRulesRequest: ListManagedInsightRulesRequest?): ListManagedInsightRulesResult? {
        return null
    }

    override fun listMetricStreams(listMetricStreamsRequest: ListMetricStreamsRequest?): ListMetricStreamsResult? {
        return null
    }

    override fun putDashboard(putDashboardRequest: PutDashboardRequest?): PutDashboardResult? {
        return null
    }

    override fun putInsightRule(putInsightRuleRequest: PutInsightRuleRequest?): PutInsightRuleResult? {
        return null
    }

    override fun putManagedInsightRules(putManagedInsightRulesRequest: PutManagedInsightRulesRequest?): PutManagedInsightRulesResult? {
        return null
    }
}