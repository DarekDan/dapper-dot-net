#region

using System;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using NUnit.Framework;

#endregion

namespace Dapper.NET45.Tests
{
    [TestFixture]
    public class UnitTests
    {
        public static readonly string ConnectionString = "Data Source=.;Initial Catalog=tempdb;Integrated Security=True";
        private string[] SequenceAbcDef = new[] {"abc", "def"};

        [TestCase(true)]
        [TestCase(false)]
        public void BasicStringUsageAsync(bool opened)
        {
            using (var connection = Helper.GetConnection(opened))
            {
                var query = connection.QueryAsync<string>("select 'abc' as [Value] union all select @txt", new {txt = "def"});
                var arr = query.Result.ToArray();
                Assert.IsTrue(arr.SequenceEqual(SequenceAbcDef));
            }
        }

        [Test]
        public void BasicStringUsageAsyncNonBuffered()
        {
            using (var connection = Helper.GetConnection())
            {
                var query =
                    connection.QueryAsync<string>(new CommandDefinition("select 'abc' as [Value] union all select @txt", new {txt = "def"},
                        flags: CommandFlags.None));
                var arr = query.Result.ToArray();
                Assert.IsTrue(arr.SequenceEqual(SequenceAbcDef));
            }
        }

        [Test]
        public void ClassWithStringUsageAsync()
        {
            using (var connection = Helper.GetConnection())
            {
                var query = connection.QueryAsync<BasicType>("select 'abc' as [Value] union all select @txt", new {txt = "def"});
                var arr = query.Result.ToArray();
                Assert.IsTrue(arr.Select(x => x.Value).SequenceEqual(SequenceAbcDef));
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        public void ExecuteAsync(bool opened)
        {
            using (var connection = Helper.GetConnection(opened))
            {
                var query = connection.ExecuteAsync("declare @foo table(id int not null); insert @foo values(@id);", new {id = 1});
                var val = query.Result;
                Assert.AreEqual(val, 1);
            }
        }

        [Test]
        public void LongOperationWithCancellation()
        {
            using (var connection = Helper.GetConnection(false))
            {
                var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                var task = connection.QueryAsync<int>(new CommandDefinition("waitfor delay '00:00:10';select 1", cancellationToken: cancel.Token));
                try
                {
                    if (!task.Wait(TimeSpan.FromSeconds(7)))
                    {
                        throw new TimeoutException(); // should have cancelled
                    }
                }
                catch (AggregateException agg)
                {
                    Assert.IsTrue(agg.InnerException is SqlException);
                }
            }
        }

        [Test]
        public void MultiMapArbitraryWithSplitAsync()
        {
            var sql = @"select 1 as id, 'abc' as name, 2 as id, 'def' as name";
            using (var connection = Helper.GetConnection())
            {
                var productQuery = connection.QueryAsync(sql, new[] {typeof (Product), typeof (Category)}, objects =>
                {
                    var prod = (Product) objects[0];
                    prod.Category = (Category) objects[1];
                    return prod;
                });

                var product = productQuery.Result.First();
                Assert.AreEqual(product.Id, 1);
                Assert.AreEqual(product.Name, "abc");
                Assert.AreEqual(product.Category.Id, 2);
                Assert.AreEqual(product.Category.Name, "def");
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        public void MultiMapWithSplitAsync(bool opened)
        {
            var sql = @"select 1 as id, 'abc' as name, 2 as id, 'def' as name";
            using (var connection = Helper.GetConnection(opened))
            {
                var productQuery = connection.QueryAsync<Product, Category, Product>(sql, (prod, cat) =>
                {
                    prod.Category = cat;
                    return prod;
                });

                var product = productQuery.Result.First();
                Assert.AreEqual(product.Id, 1);
                Assert.AreEqual(product.Name, "abc");
                Assert.AreEqual(product.Category.Id, 2);
                Assert.AreEqual(product.Category.Name, "def");
            }
        }


        [TestCase(true)]
        [TestCase(false)]
        public void MultiConnAsync(bool opened)
        {
            using (var conn = Helper.GetConnection(opened))
            {
                using (var multi = conn.QueryMultipleAsync("select 1; select 2").Result)
                {
                    Assert.AreEqual(multi.ReadAsync<int>().Result.Single(), 1);
                    Assert.AreEqual(multi.ReadAsync<int>().Result.Single(), 2);
                }
            }
        }

        [Test]
        public void QueryDynamicAsync()
        {
            using (var connection = Helper.GetConnection(false))
            {
                var row = connection.QueryAsync("select 'abc' as [Value]").Result.Single();
                string value = row.Value;
                Assert.AreEqual(value, "abc");
            }
        }

        private class BasicType
        {
            public string Value { get; set; }
        }

        private class Category
        {
            public string Description { get; set; }
            public int Id { get; set; }
            public string Name { get; set; }
        }

        private static class Helper
        {
            public static SqlConnection GetConnection(bool opened = true, bool mars = false)
            {
                var cs = ConnectionString;
                if (mars)
                {
                    var scsb = new SqlConnectionStringBuilder(cs);
                    scsb.MultipleActiveResultSets = true;
                    cs = scsb.ConnectionString;
                }
                var connection = new SqlConnection(cs);
                if (opened) connection.Open();
                return connection;
            }
        }

        private class Product
        {
            public Category Category { get; set; }
            public int Id { get; set; }
            public string Name { get; set; }
        }
    }
}