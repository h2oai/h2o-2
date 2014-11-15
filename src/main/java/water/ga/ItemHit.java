/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package water.ga;

import static water.ga.GoogleAnalyticsParameter.CURRENCY_CODE;
import static water.ga.GoogleAnalyticsParameter.ITEM_CATEGORY;
import static water.ga.GoogleAnalyticsParameter.ITEM_CODE;
import static water.ga.GoogleAnalyticsParameter.ITEM_NAME;
import static water.ga.GoogleAnalyticsParameter.ITEM_PRICE;
import static water.ga.GoogleAnalyticsParameter.ITEM_QUANTITY;
import static water.ga.GoogleAnalyticsParameter.TRANSACTION_ID;

/**
 * GA request to track items as part of ecommerce transaction.
 *
 * <p>For more information, see <a href="https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters#ecomm">GA Parameters Reference</a></p>
 *
 * @author Santhosh Kumar
 *
 * This copy of google-analytics-java is a back port of version 1.1.1 of the library.
 * This backport removes the slf4j dependency, and modifies the code to work with the
 * 4.1 version of the Apache http client library.
 *
 * Original sources can be found at https://github.com/brsanthu/google-analytics-java.
 * All copyrights retained by original authors.
 */
public class ItemHit extends GoogleAnalyticsRequest<ItemHit> {

  public ItemHit() {
    super("item");
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		<strong>Required for transaction hit type.</strong>
   * 		<br>
   * 		<strong>Required for item hit type.</strong>
   * 	</p>
   * 	<p>A unique identifier for the transaction. This value should be the same for both the Transaction hit and Items hits associated to the particular transaction.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>ti</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>500 Bytes
   * 				</td>
   * 				<td>transaction, item</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>OD564</code><br>
   * 		Example usage: <code>ti=OD564</code>
   * 	</div>
   * </div>
   */
  public ItemHit txId(String value) {
    setString(TRANSACTION_ID, value);
    return this;
  }
  public String txId() {
    return getString(TRANSACTION_ID);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		<strong>Required for item hit type.</strong>
   * 	</p>
   * 	<p>Specifies the item name.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>in</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>500 Bytes
   * 				</td>
   * 				<td>item</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>Shoe</code><br>
   * 		Example usage: <code>in=Shoe</code>
   * 	</div>
   * </div>
   */
  public ItemHit itemName(String value) {
    setString(ITEM_NAME, value);
    return this;
  }
  public String itemName() {
    return getString(ITEM_NAME);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the price for a single item / unit.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>ip</code></td>
   * 				<td>currency</td>
   * 				<td><code>0</code>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>item</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>3.50</code><br>
   * 		Example usage: <code>ip=3.50</code>
   * 	</div>
   * </div>
   */
  public ItemHit itemPrice(Double value) {
    setDouble(ITEM_PRICE, value);
    return this;
  }
  public Double itemPrice() {
    return getDouble(ITEM_PRICE);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the number of items purchased.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>iq</code></td>
   * 				<td>integer</td>
   * 				<td><code>0</code>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>item</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>4</code><br>
   * 		Example usage: <code>iq=4</code>
   * 	</div>
   * </div>
   */
  public ItemHit itemQuantity(Integer value) {
    setInteger(ITEM_QUANTITY, value);
    return this;
  }
  public Integer itemQuantity() {
    return getInteger(ITEM_QUANTITY);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the SKU or item code.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>ic</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>500 Bytes
   * 				</td>
   * 				<td>item</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>SKU47</code><br>
   * 		Example usage: <code>ic=SKU47</code>
   * 	</div>
   * </div>
   */
  public ItemHit itemCode(String value) {
    setString(ITEM_CODE, value);
    return this;
  }
  public String itemCode() {
    return getString(ITEM_CODE);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the category that the item belongs to.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>iv</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>500 Bytes
   * 				</td>
   * 				<td>item</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>Blue</code><br>
   * 		Example usage: <code>iv=Blue</code>
   * 	</div>
   * </div>
   */
  public ItemHit itemCategory(String value) {
    setString(ITEM_CATEGORY, value);
    return this;
  }

  public String itemCategory() {
    return getString(ITEM_CATEGORY);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>When present indicates the local currency for all transaction currency values. Value should be a valid ISO 4217 currency code.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>cu</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>10 Bytes
   * 				</td>
   * 				<td>transaction, item</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>EUR</code><br>
   * 		Example usage: <code>cu=EUR</code>
   * 	</div>
   * </div>
   */
  public ItemHit currencyCode(String value) {
    setString(CURRENCY_CODE, value);
    return this;
  }
  public String currencyCode() {
    return getString(CURRENCY_CODE);
  }

}
