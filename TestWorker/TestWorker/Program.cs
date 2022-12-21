namespace TestWorker
{
   internal class Program
   {
      static void Main(string[] args)
      {
         var testWorker = new TestWorker();
         testWorker.Register();
         Thread.Sleep(100);
         while (true)
         {
            testWorker.SendMessage(Console.ReadLine() ?? string.Empty);
         }

      }
   }
}