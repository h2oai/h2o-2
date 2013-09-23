package com.example.tests;

import java.util.regex.Pattern;
import java.util.concurrent.TimeUnit;
import org.junit.*;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import org.openqa.selenium.*;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.support.ui.Select;

public class Test24 {
  private WebDriver driver;
  private String baseUrl;
  private boolean acceptNextAlert = true;
  private StringBuffer verificationErrors = new StringBuffer();

  @Before
  public void setUp() throws Exception {
    driver = new FirefoxDriver();
    baseUrl = "http://0.0.0.0:11185";
    driver.manage().timeouts().implicitlyWait(30, TimeUnit.SECONDS);
  }

  @Test
  public void test24() throws Exception {
    driver.get(baseUrl + "/");
    driver.get(baseUrl + "/ImportUrl.html");
    driver.findElement(By.id("url")).clear();
    driver.findElement(By.id("url")).sendKeys("http://0.0.0.0:11185/datasets/iris.csv");
    driver.findElement(By.id("key")).clear();
    driver.findElement(By.id("key")).sendKeys("iris22.csv");
    driver.findElement(By.xpath("(//button[@onclick='query_submit()'])[2]")).click();
    driver.findElement(By.linkText("iris22.csv")).click();
    driver.findElement(By.linkText("Parse into hex format")).click();
    driver.findElement(By.cssSelector("button.btn.btn-primary")).click();
    driver.get(baseUrl + "/GBM.query?key=iris22.hex");
    driver.findElement(By.id("source")).clear();
    driver.findElement(By.id("source")).sendKeys("iris22.hex");
    driver.findElement(By.id("destination_key")).clear();
    driver.findElement(By.id("destination_key")).sendKeys("");
    new Select(driver.findElement(By.id("vresponse"))).selectByVisibleText("class");
    driver.findElement(By.xpath("(//button[@onclick='query_submit()'])[2]")).click();
  }

  @After
  public void tearDown() throws Exception {
    driver.quit();
    String verificationErrorString = verificationErrors.toString();
    if (!"".equals(verificationErrorString)) {
      fail(verificationErrorString);
    }
  }

  private boolean isElementPresent(By by) {
    try {
      driver.findElement(by);
      return true;
    } catch (NoSuchElementException e) {
      return false;
    }
  }

  private boolean isAlertPresent() {
    try {
      driver.switchTo().alert();
      return true;
    } catch (NoAlertPresentException e) {
      return false;
    }
  }

  private String closeAlertAndGetItsText() {
    try {
      Alert alert = driver.switchTo().alert();
      String alertText = alert.getText();
      if (acceptNextAlert) {
        alert.accept();
      } else {
        alert.dismiss();
      }
      return alertText;
    } finally {
      acceptNextAlert = true;
    }
  }
}
