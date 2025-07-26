using System.Data;
using Ormamu;

namespace OrmamuTests.DbProviders;

public interface IDbProvider
{
    void DeploySchema();
    IDbConnection GetConnection();
    void DeployTestData();
    void WipeCreateTestsData();
    OrmamuOptions Options { get; }
}